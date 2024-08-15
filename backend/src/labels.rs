use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};
use utoipa::ToSchema;

use crate::errors::MauveError;

#[derive(
    Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, ToSchema,
)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_ascii_lowercase(),
            value: value.to_ascii_lowercase(),
        }
    }

    #[inline(always)]
    pub fn to_fwd(&self) -> String {
        format!("{}={}", self.name, self.value)
    }

    #[inline(always)]
    pub fn to_rev(&self) -> String {
        format!("{}={}", self.value, self.name)
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.name, self.value)
    }
}

impl FromStr for Label {
    type Err = MauveError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('=') {
            Some((name, value)) => Ok(Self::new(name, value)),
            None => Err(MauveError::InvalidLabel(s.to_string())),
        }
    }
}
