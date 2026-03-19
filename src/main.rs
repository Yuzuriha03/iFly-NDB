mod common;
mod enroute;
mod geomag;
mod layout;
mod terminal;

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rusqlite::Connection;

#[derive(Debug, Parser)]
#[command(author, version, about = "Convert Fenix navdata to iFly Supplemental format")]
struct Cli {
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[arg(long)]
    csv_path: Option<PathBuf>,
    #[arg(long)]
    route_file: Option<PathBuf>,
    #[arg(long)]
    navdata_path: Option<PathBuf>,
    #[arg(long)]
    start_terminal_id: Option<i64>,
    #[arg(long)]
    end_terminal_id: Option<i64>,
    #[arg(long)]
    skip_layout_update: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    let db_path = resolve_db_path(cli.db_path)?;
    let db_connection_task = spawn_db_connection_task(db_path.clone());
    let navdata_detect_task = spawn_navdata_detect_task(cli.route_file.as_ref(), cli.navdata_path.as_ref());

    let csv_path = match cli.csv_path {
        Some(path) => path,
        None => common::prompt_path("请输入NAIP RTE_SEG.csv文件路径：", "RTE_SEG.csv")?,
    };
    let enroute_prepare_task = spawn_enroute_prepare_task(db_path.clone(), csv_path);

    let (start_terminal_id, end_terminal_id) = common::resolve_terminal_range(
        cli.start_terminal_id,
        cli.end_terminal_id,
    )?;
    let terminal_prepare_task = spawn_terminal_prepare_task(
        db_path,
        start_terminal_id,
        end_terminal_id,
    );

    let navdata_targets = resolve_navdata_targets(
        cli.route_file.as_ref(),
        cli.navdata_path.as_ref(),
        navdata_detect_task,
    )?;
    let _validated_conn = join_worker(db_connection_task, "数据库连接与校验任务")??;
    let prepared_enroute = join_worker(enroute_prepare_task, "Enroute 预加载任务")??.map(Arc::new);
    let prepared_terminal = Arc::new(
        join_worker(terminal_prepare_task, "Terminals 预加载任务")??,
    );

    common::announce_navdata_targets(&navdata_targets);

    let multiple_targets = navdata_targets.len() > 1;
    let thread_count = directory_worker_count(navdata_targets.len());
    if multiple_targets {
        println!("检测到 {} 个 navdata 目录", navdata_targets.len());
    }

    if thread_count <= 1 {
        for target in &navdata_targets {
            process_navdata_target(
                target,
                prepared_enroute.as_deref(),
                prepared_terminal.as_ref(),
                cli.skip_layout_update,
            )?;
        }
    } else {
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .context("无法创建目录级线程池")?;

        let results = pool.install(|| {
            navdata_targets
                .par_iter()
                .map(|target| {
                    process_navdata_target(
                        target,
                        prepared_enroute.as_deref(),
                        prepared_terminal.as_ref(),
                        cli.skip_layout_update,
                    )
                })
                .collect::<Vec<_>>()
        });

        for result in results {
            result?;
        }
    }

    common::countdown_timer(10);

    Ok(())
}

fn resolve_db_path(cli_db_path: Option<PathBuf>) -> Result<PathBuf> {
    cli_db_path.map_or_else(
        || common::prompt_path("请输入Fenix的nd.db3文件路径：", ".db3"),
        Ok,
    )
}

fn spawn_db_connection_task(db_path: PathBuf) -> JoinHandle<Result<Connection>> {
    thread::spawn(move || common::open_fenix_connection(&db_path))
}

fn spawn_enroute_prepare_task(
    db_path: PathBuf,
    csv_path: PathBuf,
) -> JoinHandle<Result<Option<enroute::PreparedEnrouteData>>> {
    thread::spawn(move || {
        let conn = common::open_fenix_connection(&db_path)
            .with_context(|| format!("Enroute 预加载时无法连接数据库: {}", db_path.display()))?;
        enroute::prepare(&conn, &csv_path)
            .with_context(|| format!("Enroute 预加载失败: {}", csv_path.display()))
    })
}

fn spawn_navdata_detect_task(
    route_file: Option<&PathBuf>,
    navdata_path: Option<&PathBuf>,
) -> Option<JoinHandle<Result<Vec<common::NavdataTarget>>>> {
    if route_file.is_some() || navdata_path.is_some() {
        return None;
    }

    Some(thread::spawn(common::auto_detect_navdata_paths))
}

fn spawn_terminal_prepare_task(
    db_path: PathBuf,
    start_terminal_id: i64,
    end_terminal_id: i64,
) -> JoinHandle<Result<terminal::PreparedTerminalData>> {
    thread::spawn(move || {
        let conn = common::open_fenix_connection(&db_path)
            .with_context(|| format!("Terminals 预加载时无法连接数据库: {}", db_path.display()))?;
        terminal::prepare(&conn, start_terminal_id, end_terminal_id).with_context(|| {
            format!("Terminals 预加载失败: TerminalID {start_terminal_id}-{end_terminal_id}")
        })
    })
}

fn resolve_navdata_targets(
    route_file: Option<&PathBuf>,
    navdata_path: Option<&PathBuf>,
    navdata_detect_task: Option<JoinHandle<Result<Vec<common::NavdataTarget>>>>,
) -> Result<Vec<common::NavdataTarget>> {
    if route_file.is_some() || navdata_path.is_some() {
        return common::resolve_navdata_paths(route_file.cloned(), navdata_path.cloned());
    }

    match navdata_detect_task {
        Some(task) => match join_worker(task, "navdata 自动探测任务")? {
            Ok(targets) => Ok(targets),
            Err(error) => {
                eprintln!("navdata 自动探测失败，将切换为手动输入: {error:#}");
                common::resolve_navdata_paths(None, None)
            }
        },
        None => common::resolve_navdata_paths(None, None),
    }
}

fn join_worker<T>(handle: JoinHandle<Result<T>>, task_name: &str) -> Result<Result<T>> {
    handle
        .join()
        .map_err(|payload| anyhow!("{task_name}发生线程 panic: {}", panic_payload_to_string(payload)))
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => payload
            .downcast::<&'static str>()
            .map_or_else(|_| "unknown panic payload".to_string(), |message| (*message).to_string()),
    }
}

fn process_navdata_target(
    target: &common::NavdataTarget,
    prepared_enroute: Option<&enroute::PreparedEnrouteData>,
    prepared_terminals: &terminal::PreparedTerminalData,
    skip_layout_update: bool,
) -> Result<()> {
    let target_label = target.source_label.as_str();

    if let Some(prepared_enroute) = prepared_enroute {
        enroute::write_prepared(prepared_enroute, &target.route_file, &target.navdata_path)
            .with_context(|| format!("处理 Enroute 失败: {}", target.navdata_path.display()))?;
        println!("[{target_label}] Enroute数据转换完毕");
    }

    terminal::write_prepared(prepared_terminals, &target.navdata_path)
        .with_context(|| format!("处理 Terminal 失败: {}", target.navdata_path.display()))?;
    println!("[{target_label}] Terminal数据转换完毕");

    common::delete_data_navdatasupplemental(&target.navdata_path);
    if !skip_layout_update && !target_label.starts_with("MSFS2024") {
        common::update_layout_json(&target.navdata_path)?;
    }
    Ok(())
}

fn directory_worker_count(target_count: usize) -> usize {
    if target_count <= 1 {
        return 1;
    }

    let available = thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    available.min(target_count).clamp(1, 4)
}
