use std::fmt;

use apache_avro::AvroSchema;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, AvroSchema)]
pub struct EWMA {
    pub period: i64,
    pub alpha: f64,
    pub current: f64,
}

impl EWMA {
    pub fn new(period: i64) -> Self {
        Self {
            period,
            alpha: 2.0 / (period + 1) as f64,
            current: 0.0,
        }
    }
    #[inline(always)]
    pub fn next(&mut self, x: &f64) -> f64 {
        self.current = self.alpha * (*x) + (1.0 - self.alpha) * self.current;
        self.current
    }
}

impl Default for EWMA {
    fn default() -> Self {
        Self::new(15)
    }
}

impl fmt::Display for EWMA {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EMA(current={}, alpha={}. n={})",
            self.period, self.alpha, self.period
        )
    }
}
