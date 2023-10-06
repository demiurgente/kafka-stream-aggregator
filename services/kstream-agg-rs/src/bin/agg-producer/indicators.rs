use std::fmt;

use serde_derive::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct EWMA {
    #[serde(skip)]
    period: u64,
    #[serde(skip)]
    k: f64,
    #[serde(rename(deserialize = "price"))]
    pub current: f64,
    #[serde(skip)]
    is_new: bool,
}

impl EWMA {
    pub fn new(period: u64) -> Self {
        Self {
            period,
            k: 2.0 / (period + 1) as f64,
            current: 0.0,
            is_new: true,
        }
    }

    pub fn next(&mut self, input: &f64) -> f64 {
        if self.is_new {
            self.is_new = false;
            self.current = *input;
        } else {
            self.current = self.k * *input + (1.0 - self.k) * self.current;
        }
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
        write!(f, "EMA({})", self.period)
    }
}
