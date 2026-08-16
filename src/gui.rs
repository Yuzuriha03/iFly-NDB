use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use eframe::egui::{self, Color32, RichText, TextEdit};

pub fn launch() -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([780.0, 640.0]),
        ..Default::default()
    };
    eframe::run_native(
        "iFly NDB Converter",
        options,
        Box::new(|_| Ok(Box::<ConverterApp>::default())),
    )
    .map_err(|error| anyhow::anyhow!("GUI 启动失败: {error}"))
}

struct ConverterApp {
    db_path: String,
    csv_path: String,
    navdata_path: String,
    override_path: String,
    start_id: String,
    end_id: String,
    batch_size: String,
    skip_layout: bool,
    running: bool,
    status: Arc<Mutex<String>>,
}

impl Default for ConverterApp {
    fn default() -> Self {
        Self {
            db_path: String::new(),
            csv_path: String::new(),
            navdata_path: String::new(),
            override_path: String::new(),
            start_id: String::new(),
            end_id: String::new(),
            batch_size: "500".to_string(),
            skip_layout: true,
            running: false,
            status: Arc::new(Mutex::new("等待选择数据文件。".to_string())),
        }
    }
}

impl eframe::App for ConverterApp {
    fn update(&mut self, context: &egui::Context, _: &mut eframe::Frame) {
        context.request_repaint_after(std::time::Duration::from_millis(250));
        egui::CentralPanel::default().show(context, |ui| {
            ui.visuals_mut().widgets.noninteractive.bg_fill = Color32::from_rgb(245, 246, 242);
            ui.add_space(12.0);
            ui.heading(
                RichText::new("iFly NDB Converter")
                    .size(28.0)
                    .color(Color32::from_rgb(30, 49, 43)),
            );
            ui.label(RichText::new("2608 转换 / 批量终端 / 高度修正").color(Color32::DARK_GRAY));
            ui.add_space(18.0);

            file_field(ui, "Fenix nd.db3", &mut self.db_path, true);
            file_field(ui, "RTE_SEG.csv", &mut self.csv_path, true);
            file_field(ui, "iFly navdata", &mut self.navdata_path, false);
            file_field(ui, "高度修正规则（可选）", &mut self.override_path, true);

            ui.add_space(12.0);
            ui.horizontal(|ui| {
                ui.label("Terminal ID 范围");
                ui.add(
                    TextEdit::singleline(&mut self.start_id)
                        .hint_text("起始 ID")
                        .desired_width(105.0),
                );
                ui.label("至");
                ui.add(
                    TextEdit::singleline(&mut self.end_id)
                        .hint_text("结束 ID")
                        .desired_width(105.0),
                );
                ui.label("每批");
                ui.add(TextEdit::singleline(&mut self.batch_size).desired_width(72.0));
                ui.label("条");
            });
            ui.label(
                RichText::new("留空范围则按工具默认锚点一次转换；填写范围后按批次自动连续运行。")
                    .small()
                    .color(Color32::DARK_GRAY),
            );
            ui.checkbox(
                &mut self.skip_layout,
                "不更新 layout.json（MSFS 2024 推荐）",
            );

            ui.add_space(14.0);
            if self.running {
                ui.add_enabled(false, egui::Button::new("转换进行中…"));
            } else if ui.button(RichText::new("开始转换").strong()).clicked() {
                match self.start() {
                    Ok(()) => self.running = true,
                    Err(error) => *self.status.lock().unwrap() = format!("配置错误：{error:#}"),
                }
            }

            ui.add_space(16.0);
            ui.separator();
            ui.label(RichText::new("运行日志").strong());
            let mut status = self.status.lock().unwrap().clone();
            ui.add(
                TextEdit::multiline(&mut status)
                    .desired_rows(16)
                    .interactive(false)
                    .font(egui::TextStyle::Monospace),
            );
            if self.running && status.contains("\n完成。") || status.starts_with("失败：") {
                self.running = false;
            }
        });
    }
}

impl ConverterApp {
    fn start(&mut self) -> Result<()> {
        if self.db_path.trim().is_empty()
            || self.csv_path.trim().is_empty()
            || self.navdata_path.trim().is_empty()
        {
            anyhow::bail!("必须选择 nd.db3、RTE_SEG.csv 和 iFly navdata 目录");
        }
        let start = parse_optional_id(&self.start_id, "起始 Terminal ID")?;
        let end = parse_optional_id(&self.end_id, "结束 Terminal ID")?;
        if start.is_some() != end.is_some() {
            anyhow::bail!("批量转换需要同时填写起始和结束 Terminal ID");
        }
        if let (Some(start), Some(end)) = (start, end) {
            if end < start {
                anyhow::bail!("结束 Terminal ID 不能小于起始值");
            }
        }
        let batch = self.batch_size.trim().parse::<i64>().unwrap_or(0);
        if start.is_some() && batch <= 0 {
            anyhow::bail!("批次大小必须是正整数");
        }

        let request = ConversionRequest {
            db_path: PathBuf::from(self.db_path.trim()),
            csv_path: PathBuf::from(self.csv_path.trim()),
            navdata_path: PathBuf::from(self.navdata_path.trim()),
            override_path: (!self.override_path.trim().is_empty())
                .then(|| PathBuf::from(self.override_path.trim())),
            start,
            end,
            batch,
            skip_layout: self.skip_layout,
        };
        let status = Arc::clone(&self.status);
        *status.lock().unwrap() = "开始转换…".to_string();
        std::thread::spawn(move || run_requests(request, status));
        Ok(())
    }
}

struct ConversionRequest {
    db_path: PathBuf,
    csv_path: PathBuf,
    navdata_path: PathBuf,
    override_path: Option<PathBuf>,
    start: Option<i64>,
    end: Option<i64>,
    batch: i64,
    skip_layout: bool,
}

fn run_requests(request: ConversionRequest, status: Arc<Mutex<String>>) {
    let executable = match std::env::current_exe() {
        Ok(value) => value,
        Err(error) => {
            *status.lock().unwrap() = format!("失败：无法定位当前程序：{error}");
            return;
        }
    };
    let ranges = batch_ranges(request.start, request.end, request.batch);

    let total_batches = ranges.len();
    let mut log = String::new();
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        if start == 0 {
            log.push_str("运行默认 Terminal 范围…\n");
        } else {
            log.push_str(&format!("批次 {}：Terminal ID {start}–{end}\n", index + 1));
        }
        *status.lock().unwrap() = log.clone();
        let mut command = Command::new(&executable);
        command
            .arg("--db-path")
            .arg(&request.db_path)
            .arg("--csv-path")
            .arg(&request.csv_path)
            .arg("--navdata-path")
            .arg(&request.navdata_path)
            .arg("--no-countdown");
        if request.skip_layout {
            command.arg("--skip-layout-update");
        }
        // A correction file can contain sections from any batch, therefore it
        // is applied only after the final batch has written every procedure.
        if index + 1 == total_batches {
            if let Some(path) = &request.override_path {
                command.arg("--altitude-overrides").arg(path);
            }
        }
        if start != 0 {
            command.arg("--start-terminal-id").arg(start.to_string());
            command.arg("--end-terminal-id").arg(end.to_string());
        }
        match command.output() {
            Ok(output) if output.status.success() => {
                log.push_str(&String::from_utf8_lossy(&output.stdout));
            }
            Ok(output) => {
                log.push_str(&String::from_utf8_lossy(&output.stdout));
                log.push_str(&String::from_utf8_lossy(&output.stderr));
                *status.lock().unwrap() = format!("失败：\n{log}");
                return;
            }
            Err(error) => {
                *status.lock().unwrap() = format!("失败：无法启动转换器：{error}");
                return;
            }
        }
    }
    log.push_str("\n完成。");
    *status.lock().unwrap() = log;
}

fn parse_optional_id(value: &str, label: &str) -> Result<Option<i64>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        value
            .parse()
            .map_err(|_| anyhow::anyhow!("{label} 必须是整数"))?,
    ))
}

fn batch_ranges(start: Option<i64>, end: Option<i64>, batch_size: i64) -> Vec<(i64, i64)> {
    match (start, end) {
        (Some(start), Some(end)) => (start..=end)
            .step_by(batch_size as usize)
            .map(|first| (first, (first + batch_size - 1).min(end)))
            .collect(),
        _ => vec![(0, 0)],
    }
}

fn file_field(ui: &mut egui::Ui, label: &str, value: &mut String, file: bool) {
    ui.horizontal(|ui| {
        ui.label(label).on_hover_text(label);
        ui.add(TextEdit::singleline(value).desired_width(510.0));
        if ui.button("选择").clicked() {
            let dialog = rfd::FileDialog::new();
            let selected = if file {
                dialog.pick_file()
            } else {
                dialog.pick_folder()
            };
            if let Some(path) = selected {
                *value = path.display().to_string();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::batch_ranges;

    #[test]
    fn partitions_terminal_range_without_gaps_or_overlap() {
        assert_eq!(
            batch_ranges(Some(100), Some(1_099), 500),
            vec![(100, 599), (600, 1_099)]
        );
    }
}
