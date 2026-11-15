use super::Tcbs;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt};
use trading_bot_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};

/// Type that defines how to translate a Trading Bot [`Subscription`] into a [`Tcbs`] market
/// (stock symbol, eg/ "TCB") that can be subscribed to.
///
/// TCBS symbols have no quote asset (everything trades in VND), so only the base asset is used.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct TcbsMarket(pub SmolStr);

impl<Kind> Identifier<TcbsMarket> for Subscription<Tcbs, MarketDataInstrument, Kind> {
    fn id(&self) -> TcbsMarket {
        tcbs_market(&self.instrument.base)
    }
}

impl<InstrumentKey, Kind> Identifier<TcbsMarket>
    for Subscription<Tcbs, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> TcbsMarket {
        tcbs_market(&self.instrument.value.base)
    }
}

impl<InstrumentKey, Kind> Identifier<TcbsMarket>
    for Subscription<Tcbs, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> TcbsMarket {
        TcbsMarket(self.instrument.name_exchange.name().to_uppercase_smolstr())
    }
}

impl AsRef<str> for TcbsMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for TcbsMarket {
    fn from(symbol: &str) -> Self {
        Self(symbol.trim().to_uppercase_smolstr())
    }
}

fn tcbs_market(base: &AssetNameInternal) -> TcbsMarket {
    TcbsMarket(base.as_ref().to_uppercase_smolstr())
}
