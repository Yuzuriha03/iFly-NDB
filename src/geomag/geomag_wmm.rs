#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::io::{BufRead, BufReader};
use std::{fs, process};

const WMM_VALIDITY_RANGE: f64 = 5.0;
const A: f64 = 6378137.0;
const f: f64 = 1.0 / 298.257223563;
const e_2: f64 = f * (2.0 - f);
const a: f64 = 6371200.0;

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

fn factorial(n: usize) -> f64 {
    (1..=n).fold(1.0, |acc, x| acc * x as f64)
}

fn legendre_polynomial(t: f64, n: usize, m: usize) -> f64 {
    let mut p = 0.0;
    let mut k = 0;
    while k <= (n - m) / 2 {
        let num = (-1.0f64).powi(k.try_into().unwrap())
            * factorial(2 * n - 2 * k)
            * t.powi(n as i32 - m as i32 - 2 * k as i32);
        let den = factorial(k) * factorial(n - k) * factorial(n - m - 2 * k);
        p += num / den;
        k += 1;
    }
    p *= (2.0f64).powi(-(n as i32)) * (1.0 - t * t).powf(m as f64 / 2.0);
    p
}

fn schmidt_semi_normalised_associated_legendre(mu: f64) -> [[f64; 14]; 14] {
    let mut psn = [[0.0; 14]; 14];
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

pub enum Error {
    DateOutsideOfValidityRange,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::DateOutsideOfValidityRange => write!(formatter, "Date outside of validity range!"),
        }
    }
}

pub struct MagneticModel {
    from_year: f64,
    g: [[f64; 13]; 13],
    h: [[f64; 13]; 13],
    g_sv: [[f64; 13]; 13],
    h_sv: [[f64; 13]; 13],
}

impl MagneticModel {
    pub fn calculate(
        &self,
        h: f64,
        phi: f64,
        lambda: f64,
        year: f64,
    ) -> Result<MagneticCalculation, Error> {
        let lambda = lambda.to_radians();
        let phi = phi.to_radians();
        let r_c = A / f64::sqrt(1.0 - e_2 * f64::sin(phi) * f64::sin(phi));
        let p = (r_c + h) * f64::cos(phi);
        let z = (r_c * (1.0 - e_2) + h) * f64::sin(phi);
        let r = f64::sqrt(p * p + z * z);
        let phi_prime = f64::asin(z / r);

        let psn_sinphi = schmidt_semi_normalised_associated_legendre(f64::sin(phi_prime));

        let mut g_t = [[0.0; 13]; 13];
        let mut h_t = [[0.0; 13]; 13];
        let mut dpsn_sinphi_dphi = [[0.0; 13]; 13];
        let time_delta = year - self.from_year;

        if !(0.0..=WMM_VALIDITY_RANGE).contains(&time_delta) {
            return Result::Err(Error::DateOutsideOfValidityRange);
        }

        for n in 0..=12 {
            for m in 0..=n {
                g_t[n][m] = self.g[n][m] + time_delta * self.g_sv[n][m];
                h_t[n][m] = self.h[n][m] + time_delta * self.h_sv[n][m];

                dpsn_sinphi_dphi[n][m] = (n as f64 + 1.0) * f64::tan(phi_prime) * psn_sinphi[n][m]
                    - f64::sqrt((n as f64 + 1.0).powi(2) - (m as f64).powi(2)) / f64::cos(phi_prime)
                        * psn_sinphi[n + 1][m];
            }
        }

        let mut x_prime = 0.0;
        let mut y_prime = 0.0;
        let mut z_prime = 0.0;
        let mut x_dot_prime = 0.0;
        let mut y_dot_prime = 0.0;
        let mut z_dot_prime = 0.0;

        for n in 1..=12 {
            let mut x_prime_tmp = 0.0;
            let mut y_prime_tmp = 0.0;
            let mut z_prime_tmp = 0.0;
            let mut x_dot_prime_tmp = 0.0;
            let mut y_dot_prime_tmp = 0.0;
            let mut z_dot_prime_tmp = 0.0;

            for m in 0..=n {
                x_prime_tmp +=
                    (g_t[n][m] * f64::cos(m as f64 * lambda) + h_t[n][m] * f64::sin(m as f64 * lambda))
                        * dpsn_sinphi_dphi[n][m];
                y_prime_tmp += m as f64
                    * (g_t[n][m] * f64::sin(m as f64 * lambda)
                        - h_t[n][m] * f64::cos(m as f64 * lambda))
                    * psn_sinphi[n][m];
                z_prime_tmp +=
                    (g_t[n][m] * f64::cos(m as f64 * lambda) + h_t[n][m] * f64::sin(m as f64 * lambda))
                        * psn_sinphi[n][m];
                x_dot_prime_tmp += (self.g_sv[n][m] * f64::cos(m as f64 * lambda)
                    + self.h_sv[n][m] * f64::sin(m as f64 * lambda))
                    * dpsn_sinphi_dphi[n][m];
                y_dot_prime_tmp += m as f64
                    * (self.g_sv[n][m] * f64::sin(m as f64 * lambda)
                        - self.h_sv[n][m] * f64::cos(m as f64 * lambda))
                    * psn_sinphi[n][m];
                z_dot_prime_tmp += (self.g_sv[n][m] * f64::cos(m as f64 * lambda)
                    + self.h_sv[n][m] * f64::sin(m as f64 * lambda))
                    * psn_sinphi[n][m];
            }
            let k_temp = (a / r).powi(n as i32 + 2);
            x_prime += k_temp * x_prime_tmp;
            y_prime += k_temp * y_prime_tmp;
            z_prime += (n as f64 + 1.0) * k_temp * z_prime_tmp;
            x_dot_prime += k_temp * x_dot_prime_tmp;
            y_dot_prime += k_temp * y_dot_prime_tmp;
            z_dot_prime += (n as f64 + 1.0) * k_temp * z_dot_prime_tmp;
        }
        x_prime *= -1.0;
        y_prime *= 1.0 / f64::cos(phi_prime);
        z_prime *= -1.0;
        x_dot_prime *= -1.0;
        y_dot_prime *= 1.0 / f64::cos(phi_prime);
        z_dot_prime *= -1.0;

        let x = x_prime * f64::cos(phi_prime - phi) - z_prime * f64::sin(phi_prime - phi);
        let y = y_prime;
        let z = x_prime * f64::sin(phi_prime - phi) + z_prime * f64::cos(phi_prime - phi);
        let x_dot = x_dot_prime * f64::cos(phi_prime - phi) - z_dot_prime * f64::sin(phi_prime - phi);
        let y_dot = y_dot_prime;
        let z_dot = x_dot_prime * f64::sin(phi_prime - phi) + z_dot_prime * f64::cos(phi_prime - phi);

        let h = f64::sqrt(x * x + y * y);
        let total_intensity = f64::sqrt(h * h + z * z);
        let i = f64::atan2(z, h);
        let d = f64::atan2(y, x);
        let h_dot = (x * x_dot + y * y_dot) / h;
        let f_dot = (x * x_dot + y * y_dot + z * z_dot) / total_intensity;
        let i_dot = (z_dot * h - z * h_dot) / (total_intensity * total_intensity);
        let d_dot = (y_dot * x - y * x_dot) / (h * h);

        Result::Ok((
            x,
            y,
            z,
            h,
            total_intensity,
            i.to_degrees(),
            d.to_degrees(),
            x_dot,
            y_dot,
            z_dot,
            h_dot,
            f_dot,
            i_dot.to_degrees(),
            d_dot.to_degrees(),
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

    let mut g = [[0.0; 13]; 13];
    let mut h = [[0.0; 13]; 13];
    let mut g_sv = [[0.0; 13]; 13];
    let mut h_sv = [[0.0; 13]; 13];
    for line in model_file {
        match line {
            Ok(line) => {
                if line.starts_with("9999") {
                    break;
                }

                let mut line = line.split_whitespace();

                let n_line: i32 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let m_line: i32 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let g_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let h_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let g_sv_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");
                let h_sv_line: f64 = line.next().expect("Error parsing model file!").parse().expect("Error parsing model file!");

                if n_line > 12 {
                    break;
                }

                if m_line > n_line || m_line < 0 {
                    eprintln!("Corrupt record in model file!");
                    process::exit(1);
                }

                g[n_line as usize][m_line as usize] = g_line;
                g_sv[n_line as usize][m_line as usize] = g_sv_line;

                if m_line != 0 {
                    h[n_line as usize][m_line as usize] = h_line;
                    h_sv[n_line as usize][m_line as usize] = h_sv_line;
                }
            }
            Err(e) => {
                eprintln!("Error reading model file: {:?}", e);
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