use std::io::{BufRead, BufReader};
use std::{fs, process};

const WMM_VALIDITY_RANGE: f64 = 5.0;
const EQUATORIAL_RADIUS: f64 = 6_378_137.0;
const FLATTENING: f64 = 1.0 / 298.257_223_563;
const ECCENTRICITY_SQUARED: f64 = FLATTENING * (2.0 - FLATTENING);
const REFERENCE_RADIUS: f64 = 6_371_200.0;
const MAX_DEGREE: usize = 12;
const COEFF_LEN: usize = MAX_DEGREE + 1;
const LEGENDRE_LEN: usize = MAX_DEGREE + 2;

type CoeffArray = [[f64; COEFF_LEN]; COEFF_LEN];

type MagneticCalculation = (
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
);

fn usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).expect("index out of i32 range")
}

fn usize_to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).expect("index out of u32 range"))
}

fn factorial(n: usize) -> f64 {
    (1..=n).fold(1.0, |acc, x| acc * usize_to_f64(x))
}

fn legendre_polynomial(t: f64, n: usize, m: usize) -> f64 {
    let mut p = 0.0;
    for k in 0..=((n - m) / 2) {
        let num = (-1.0f64).powi(usize_to_i32(k))
            * factorial(2 * n - 2 * k)
            * t.powi(usize_to_i32(n) - usize_to_i32(m) - 2 * usize_to_i32(k));
        let den = factorial(k) * factorial(n - k) * factorial(n - m - 2 * k);
        p += num / den;
    }
    p *= (2.0f64).powi(-usize_to_i32(n)) * t.mul_add(-t, 1.0).powf(usize_to_f64(m) / 2.0);
    p
}

fn schmidt_semi_normalised_associated_legendre(mu: f64) -> [[f64; LEGENDRE_LEN]; LEGENDRE_LEN] {
    let mut psn = [[0.0; LEGENDRE_LEN]; LEGENDRE_LEN];
    for (n, row) in psn.iter_mut().enumerate() {
        for (m, cell) in row.iter_mut().enumerate().take(n + 1) {
            if m == 0 {
                *cell = legendre_polynomial(mu, n, m);
            } else {
                *cell = f64::sqrt(2.0 * factorial(n - m) / factorial(n + m))
                    * legendre_polynomial(mu, n, m);
            }
        }
    }
    psn
}

#[derive(Debug)]
pub enum Error {
    DateOutsideOfValidityRange,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::DateOutsideOfValidityRange => write!(formatter, "Date outside of validity range!"),
        }
    }
}

impl std::error::Error for Error {}

pub struct MagneticModel {
    from_year: f64,
    g: CoeffArray,
    h: CoeffArray,
    g_sv: CoeffArray,
    h_sv: CoeffArray,
}

impl MagneticModel {
    fn validate_time_delta(&self, year: f64) -> Result<f64, Error> {
        let time_delta = year - self.from_year;
        if !(0.0..=WMM_VALIDITY_RANGE).contains(&time_delta) {
            return Err(Error::DateOutsideOfValidityRange);
        }
        Ok(time_delta)
    }

    fn build_time_adjusted_coefficients(&self, time_delta: f64) -> (CoeffArray, CoeffArray) {
        let mut g_t = [[0.0; COEFF_LEN]; COEFF_LEN];
        let mut h_t = [[0.0; COEFF_LEN]; COEFF_LEN];
        for n in 0..=MAX_DEGREE {
            for m in 0..=n {
                g_t[n][m] = time_delta.mul_add(self.g_sv[n][m], self.g[n][m]);
                h_t[n][m] = time_delta.mul_add(self.h_sv[n][m], self.h[n][m]);
            }
        }
        (g_t, h_t)
    }

    fn build_legendre_derivative(
        phi_prime: f64,
        psn_sinphi: &[[f64; LEGENDRE_LEN]; LEGENDRE_LEN],
    ) -> CoeffArray {
        let mut derivative = [[0.0; COEFF_LEN]; COEFF_LEN];
        for n in 0..=MAX_DEGREE {
            for m in 0..=n {
                let n_plus_one = usize_to_f64(n) + 1.0;
                let m_f64 = usize_to_f64(m);
                let first = n_plus_one * phi_prime.tan() * psn_sinphi[n][m];
                let second = n_plus_one.mul_add(n_plus_one, -(m_f64 * m_f64)).sqrt()
                    / phi_prime.cos()
                    * psn_sinphi[n + 1][m];
                derivative[n][m] = first - second;
            }
        }
        derivative
    }

    fn accumulate_prime_components(
        &self,
        lambda: f64,
        r: f64,
        psn_sinphi: &[[f64; LEGENDRE_LEN]; LEGENDRE_LEN],
        dpsn_sinphi_dphi: &CoeffArray,
        g_t: &CoeffArray,
        h_t: &CoeffArray,
    ) -> (f64, f64, f64, f64, f64, f64) {
        let mut x_prime = 0.0;
        let mut y_prime = 0.0;
        let mut z_prime = 0.0;
        let mut x_dot_prime = 0.0;
        let mut y_dot_prime = 0.0;
        let mut z_dot_prime = 0.0;

        for n in 1..=MAX_DEGREE {
            let mut x_prime_tmp = 0.0;
            let mut y_prime_tmp = 0.0;
            let mut z_prime_tmp = 0.0;
            let mut x_dot_prime_tmp = 0.0;
            let mut y_dot_prime_tmp = 0.0;
            let mut z_dot_prime_tmp = 0.0;

            for m in 0..=n {
                let m_f64 = usize_to_f64(m);
                let m_lambda = m_f64 * lambda;
                let cos_ml = m_lambda.cos();
                let sin_ml = m_lambda.sin();

                let coeff = g_t[n][m].mul_add(cos_ml, h_t[n][m] * sin_ml);
                let coeff_sv = self.g_sv[n][m].mul_add(cos_ml, self.h_sv[n][m] * sin_ml);
                let cross = g_t[n][m].mul_add(sin_ml, -(h_t[n][m] * cos_ml));
                let cross_sv = self.g_sv[n][m].mul_add(sin_ml, -(self.h_sv[n][m] * cos_ml));

                x_prime_tmp += coeff * dpsn_sinphi_dphi[n][m];
                y_prime_tmp += m_f64 * cross * psn_sinphi[n][m];
                z_prime_tmp += coeff * psn_sinphi[n][m];
                x_dot_prime_tmp += coeff_sv * dpsn_sinphi_dphi[n][m];
                y_dot_prime_tmp += m_f64 * cross_sv * psn_sinphi[n][m];
                z_dot_prime_tmp += coeff_sv * psn_sinphi[n][m];
            }

            let k_temp = (REFERENCE_RADIUS / r).powi(usize_to_i32(n) + 2);
            let n_plus_one = usize_to_f64(n) + 1.0;
            x_prime += k_temp * x_prime_tmp;
            y_prime += k_temp * y_prime_tmp;
            z_prime += n_plus_one * k_temp * z_prime_tmp;
            x_dot_prime += k_temp * x_dot_prime_tmp;
            y_dot_prime += k_temp * y_dot_prime_tmp;
            z_dot_prime += n_plus_one * k_temp * z_dot_prime_tmp;
        }

        (x_prime, y_prime, z_prime, x_dot_prime, y_dot_prime, z_dot_prime)
    }

    pub fn calculate(
        &self,
        altitude_m: f64,
        latitude_deg: f64,
        longitude_deg: f64,
        year: f64,
    ) -> Result<MagneticCalculation, Error> {
        let time_delta = self.validate_time_delta(year)?;
        let longitude_rad = longitude_deg.to_radians();
        let latitude_rad = latitude_deg.to_radians();
        let curvature_radius = EQUATORIAL_RADIUS
            / f64::sqrt((ECCENTRICITY_SQUARED * latitude_rad.sin()).mul_add(-latitude_rad.sin(), 1.0));
        let planar_radius = (curvature_radius + altitude_m) * f64::cos(latitude_rad);
        let vertical_offset = (curvature_radius * (1.0 - ECCENTRICITY_SQUARED) + altitude_m) * f64::sin(latitude_rad);
        let geocentric_radius = f64::sqrt(planar_radius * planar_radius + vertical_offset * vertical_offset);
        let geocentric_latitude = f64::asin(vertical_offset / geocentric_radius);

        let psn_sinphi = schmidt_semi_normalised_associated_legendre(f64::sin(geocentric_latitude));
        let (g_t, h_t) = self.build_time_adjusted_coefficients(time_delta);
        let dpsn_sinphi_dphi = Self::build_legendre_derivative(geocentric_latitude, &psn_sinphi);
        let (mut x_prime, mut y_prime, mut z_prime, mut x_dot_prime, mut y_dot_prime, mut z_dot_prime) =
            self.accumulate_prime_components(longitude_rad, geocentric_radius, &psn_sinphi, &dpsn_sinphi_dphi, &g_t, &h_t);

        x_prime *= -1.0;
        y_prime *= 1.0 / f64::cos(geocentric_latitude);
        z_prime *= -1.0;
        x_dot_prime *= -1.0;
        y_dot_prime *= 1.0 / f64::cos(geocentric_latitude);
        z_dot_prime *= -1.0;

        let delta_lat = geocentric_latitude - latitude_rad;
        let x_north = x_prime.mul_add(f64::cos(delta_lat), -(z_prime * f64::sin(delta_lat)));
        let y_east = y_prime;
        let z_down = x_prime.mul_add(f64::sin(delta_lat), z_prime * f64::cos(delta_lat));
        let x_north_dot = x_dot_prime.mul_add(f64::cos(delta_lat), -(z_dot_prime * f64::sin(delta_lat)));
        let y_east_dot = y_dot_prime;
        let z_down_dot = x_dot_prime.mul_add(f64::sin(delta_lat), z_dot_prime * f64::cos(delta_lat));

        let horizontal_intensity = f64::sqrt(x_north * x_north + y_east * y_east);
        let total_intensity = f64::sqrt(horizontal_intensity * horizontal_intensity + z_down * z_down);
        let inclination = f64::atan2(z_down, horizontal_intensity);
        let declination = f64::atan2(y_east, x_north);
        let horizontal_dot = (x_north * x_north_dot + y_east * y_east_dot) / horizontal_intensity;
        let total_dot = (x_north * x_north_dot + y_east * y_east_dot + z_down * z_down_dot) / total_intensity;
        let inclination_dot = (z_down_dot * horizontal_intensity - z_down * horizontal_dot) / total_intensity.powi(2);
        let declination_dot = y_east_dot.mul_add(x_north, -(y_east * x_north_dot)) / horizontal_intensity.powi(2);

        Ok((
            x_north,
            y_east,
            z_down,
            horizontal_intensity,
            total_intensity,
            inclination.to_degrees(),
            declination.to_degrees(),
            x_north_dot,
            y_east_dot,
            z_down_dot,
            horizontal_dot,
            total_dot,
            inclination_dot.to_degrees(),
            declination_dot.to_degrees(),
        ))
    }
}

pub fn initialise_magnetic_model(path: &str) -> MagneticModel {
    let model_file = fs::File::open(path).expect("Model file not found!");
    let mut model_file = BufReader::new(model_file).lines();

    let from_year = model_file
        .next()
        .expect("Model file is empty!")
        .expect("Error reading model file!")
        .split_whitespace()
        .next()
        .expect("Error parsing model file!")
        .parse::<f64>()
        .expect("Error parsing model file!");

    let mut g = [[0.0; COEFF_LEN]; COEFF_LEN];
    let mut h = [[0.0; COEFF_LEN]; COEFF_LEN];
    let mut g_sv = [[0.0; COEFF_LEN]; COEFF_LEN];
    let mut h_sv = [[0.0; COEFF_LEN]; COEFF_LEN];
    for line in model_file {
        match line {
            Ok(line) => {
                if line.starts_with("9999") {
                    break;
                }

                let mut line = line.split_whitespace();

                let n_line: usize = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let m_line: usize = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let g_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let h_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let g_sv_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let h_sv_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");

                if n_line > MAX_DEGREE {
                    break;
                }

                if m_line > n_line {
                    eprintln!("Corrupt record in model file!");
                    process::exit(1);
                }

                g[n_line][m_line] = g_line;
                g_sv[n_line][m_line] = g_sv_line;

                if m_line != 0 {
                    h[n_line][m_line] = h_line;
                    h_sv[n_line][m_line] = h_sv_line;
                }
            }
            Err(e) => {
                eprintln!("Error reading model file: {e:?}");
            }
        }
    }

    MagneticModel {
        from_year,
        g,
        h,
        g_sv,
        h_sv,
    }
}