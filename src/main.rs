mod common;
mod enroute;
mod geomag;
mod layout;
mod terminals;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

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
    #[arg(long, default_value_t = false)]
    skip_layout_update: bool,
    #[arg(long, default_value_t = false)]
    skip_countdown: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    let mut conn = match cli.db_path {
        Some(path) => common::open_fenix_connection(&path)?,
        None => loop {
            let path = common::prompt_path("请输入Fenix的nd.db3文件路径：", ".db3")?;
            match common::open_fenix_connection(&path) {
                Ok(conn) => break conn,
                Err(error) => eprintln!("{error}"),
            }
        },
    };

    let csv_path = match cli.csv_path {
        Some(path) => path,
        None => common::prompt_path("请输入NAIP RTE_SEG.csv文件路径：", "RTE_SEG.csv")?,
    };

    let (route_file, navdata_path, other_paths) = common::resolve_navdata_paths(
        cli.route_file,
        cli.navdata_path,
    )?;

    let (start_terminal_id, end_terminal_id) = common::resolve_terminal_range(
        cli.start_terminal_id,
        cli.end_terminal_id,
    )?;

    println!("开始处理Enroute部分");
    enroute::run(&mut conn, &route_file, &navdata_path, &csv_path)?;

    println!("开始处理Terminals部分");
    terminals::run(&conn, &navdata_path, start_terminal_id, end_terminal_id)?;

    common::delete_data_navdatasupplemental(&navdata_path)?;
    if !cli.skip_layout_update {
        common::update_layout_json(&navdata_path)?;
    }

    for target_path in other_paths {
        common::sync_navdata_to_other_path(&navdata_path, &target_path, !cli.skip_layout_update)?;
    }

    if !cli.skip_countdown {
        common::countdown_timer(10);
    }

    Ok(())
}
