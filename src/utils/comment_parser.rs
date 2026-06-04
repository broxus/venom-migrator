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

#[cfg(test)]
mod tests {
    use tycho_types::boc::Boc;

    use crate::utils::abi::UnpackAbiPlain;
    use crate::utils::token_wallets;

    use super::*;

    #[test]
    fn parse_recipient_from_tip3_accept_transfer_payload() {
        let body = Boc::decode_base64(
            "te6ccgEBBAEApQABa2eguV8AAAAAAAAAAAAAAAAAAw1AgAAwJETXtGEh+ucBw0iVv+UvTIFYNEcbPol867KVQTEBUAEBQ4AAMCRE17RhIfrnAcNIlb/lL0yBWDRHGz6JfOuylUExAUgCAQADAIQwOjgwMWEzNjNlNTFkNmYzNjVkYWE0MmI3ZDU2NGQ1OWM3ODAxMTQ4NTBjY2U1NWE0MGM5MjA5NTBlMTg2ZTcxY2M=",
        )
        .unwrap();

        let inputs = token_wallets::accept_transfer()
            .decode_internal_input(body.as_slice().unwrap())
            .unwrap();
        let transfer: token_wallets::AcceptTransferInputs = inputs.unpack().unwrap();

        let recipient = parse_recipient_address(transfer.payload.as_slice().unwrap()).unwrap();

        assert_eq!(
            recipient.to_string(),
            "0:801a363e51d6f365daa42b7d564d59c780114850cce55a40c920950e186e71cc"
        );
    }
}
