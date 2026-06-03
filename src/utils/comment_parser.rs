use std::str::FromStr;

use tycho_types::cell::CellSlice;
use tycho_types::models::StdAddr;

pub fn parse_recipient_address(payload: CellSlice<'_>) -> Option<StdAddr> {
    let mut payload = payload;
    if payload.load_u32().ok()? != 0 {
        return None;
    }

    let mut cell = payload.load_reference().ok()?;
    let mut data = Vec::new();

    loop {
        if cell.bit_len() % 8 != 0 {
            return None;
        }

        data.extend_from_slice(cell.data());

        let Some(child) = cell.reference(0) else {
            let comment = String::from_utf8(data).ok()?;
            return StdAddr::from_str(comment.trim()).ok();
        };

        cell = child;
    }
}
