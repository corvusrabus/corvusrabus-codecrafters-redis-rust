use std::collections::HashMap;
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone)]
pub struct ValueState {
    pub value: String,
    expiry: Option<u128>,
}

impl ValueState {
    pub fn no_expiry(value: String) -> Self {
        Self {
            value,
            expiry: None,
        }
    }
    pub fn with_expiry(value: String, expiry: u128) -> Self {
        let time_now = UNIX_EPOCH.elapsed().unwrap().as_millis();

        Self {
            value,
            expiry: Some(time_now + expiry),
        }
    }
    pub fn value_after_expiry(&self) -> Option<&String> {
        if let Some(expiry) = self.expiry {
            let time_now = UNIX_EPOCH.elapsed().unwrap().as_millis();
            if time_now > expiry {
                return None;
            }
        }
        Some(&self.value)
    }
}

pub type RedisState = HashMap<String, ValueState>;
