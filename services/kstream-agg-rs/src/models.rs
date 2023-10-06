use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Error as FmtError, Formatter};

#[derive(Deserialize, Serialize, Debug, Clone, AvroSchema)]
pub enum LiquidationType {
    #[serde(rename = "M")]
    Maker,
    #[serde(rename = "T")]
    Taker,
    #[serde(rename = "MT")]
    MakerTaker,
}

#[derive(Deserialize, Serialize, Debug, Clone, AvroSchema)]
pub enum Direction {
    #[allow(non_camel_case_types)]
    buy,
    #[allow(non_camel_case_types)]
    sell,
    #[allow(non_camel_case_types)]
    zero,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{:?}", self)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, AvroSchema)]
pub struct TradesDataAvro {
    pub amount: f64,
    pub direction: Direction,
    pub index_price: f64,
    pub instrument_name: String,
    pub iv: Option<f64>,
    pub liquidation: Option<LiquidationType>,
    pub price: f64,
    pub tick_direction: i64,
    pub timestamp: i64,
    pub trade_id: String,
    pub trade_seq: i64,
}
