use super::Ssi;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt};
use trading_bot_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};

/// Type that defines how to translate a Trading Bot [`Subscription`] into an
/// [`Ssi`] market (stock symbol, eg/ "VNM") that can be subscribed to.
///
/// SSI symbols have no quote asset (everything trades in VND), so only the base asset is used.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct SsiMarket(pub SmolStr);

impl<Kind> Identifier<SsiMarket> for Subscription<Ssi, MarketDataInstrument, Kind> {
    fn id(&self) -> SsiMarket {
        ssi_market(&self.instrument.base)
    }
}

impl<InstrumentKey, Kind> Identifier<SsiMarket>
    for Subscription<Ssi, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> SsiMarket {
        ssi_market(&self.instrument.value.base)
    }
}

impl<InstrumentKey, Kind> Identifier<SsiMarket>
    for Subscription<Ssi, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> SsiMarket {
        SsiMarket(self.instrument.name_exchange.name().to_uppercase_smolstr())
    }
}

impl AsRef<str> for SsiMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SsiMarket {
    fn from(symbol: &str) -> Self {
        Self(symbol.to_uppercase_smolstr())
    }
}

fn ssi_market(base: &AssetNameInternal) -> SsiMarket {
    SsiMarket(base.as_ref().to_uppercase_smolstr())
}
