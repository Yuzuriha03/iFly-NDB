use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use chrono::{Datelike, Local};

use crate::geomag::geomag_wmm::{initialise_magnetic_model, MagneticModel};

const WMM_HIGH_RESOLUTION: &str = include_str!("magcof/WMMHR.COF");

static MODEL: OnceLock<MagneticModel> = OnceLock::new();
static MODEL_PATH: OnceLock<PathBuf> = OnceLock::new();

pub(crate) fn batch_get_magnetic_variations(coordinates: &[(f64, f64)]) -> Result<Vec<f64>> {
    let model = shared_model()?;
    let decimal_year = current_decimal_year();
    coordinates
        .iter()
        .map(|&(lat, lon)| {
            let (_, _, _, _, _, _, declination, _, _, _, _, _, _, _) = model
                .calculate(0.0, lat, lon, decimal_year)
                .map_err(|error| anyhow::anyhow!(error.to_string()))
                .with_context(|| format!("geomag calculation failed for lat={lat}, lon={lon}"))?;
            Ok((declination * 10.0).round() / 10.0)
        })
        .collect()
}

fn shared_model() -> Result<&'static MagneticModel> {
    if let Some(model) = MODEL.get() {
        return Ok(model);
    }
    let model_path = ensure_model_file()?;
    let model = initialise_magnetic_model(
        model_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid geomag model path"))?,
    );
    let _ = MODEL.set(model);
    Ok(MODEL.get().expect("magnetic model initialized"))
}

fn ensure_model_file() -> Result<&'static PathBuf> {
    if let Some(path) = MODEL_PATH.get() {
        return Ok(path);
    }
    let file_path = std::env::temp_dir().join("ifly_ndb_converter_wmmhr_2025.cof");
    if !file_path.exists() {
        fs::write(&file_path, WMM_HIGH_RESOLUTION)
            .with_context(|| format!("unable to write WMM model to {}", file_path.display()))?;
    }
    let _ = MODEL_PATH.set(file_path);
    Ok(MODEL_PATH.get().expect("model path initialized"))
}

fn current_decimal_year() -> f64 {
    let now = Local::now();
    now.year() as f64 + ((now.month() as f64 - 1.0) / 12.0) + (now.day() as f64 / 365.0)
}
