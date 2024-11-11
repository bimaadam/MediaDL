//! Uncompressed ZIP archive. Names are single path segments the caller created.

use crate::application::AppError;

const MAX_ENTRIES: usize = 10;
const MAX_UNCOMPRESSED: u64 = 64 * 1024 * 1024;

pub fn write_stored_zip(files: &[(String, Vec<u8>)]) -> Result<Vec<u8>, AppError> {
    if files.is_empty() || files.len() > MAX_ENTRIES {
        return Err(AppError::Validation("photo archive has an unexpected file count".into()));
    }
    let mut locals = Vec::new();
    let mut central = Vec::new();
    let mut offset: u32 = 0;
    let mut total = 0u64;
    for (name, data) in files {
        if !valid_name(name) {
            return Err(AppError::Validation("photo archive name is invalid".into()));
        }
        total = total.saturating_add(data.len() as u64);
        if total > MAX_UNCOMPRESSED {
            return Err(AppError::TooLarge);
        }
        let crc = crc32(data);
        let size = u32::try_from(data.len()).map_err(|_| AppError::TooLarge)?;
        let name_len =
            u16::try_from(name.len()).map_err(|_| AppError::Validation("name".into()))?;
        let mut local = Vec::new();
        local.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        local.extend_from_slice(&20u16.to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(&crc.to_le_bytes());
        local.extend_from_slice(&size.to_le_bytes());
        local.extend_from_slice(&size.to_le_bytes());
        local.extend_from_slice(&name_len.to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(name.as_bytes());
        local.extend_from_slice(data);
        let mut entry = Vec::new();
        entry.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        entry.extend_from_slice(&0x0314u16.to_le_bytes());
        entry.extend_from_slice(&20u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&crc.to_le_bytes());
        entry.extend_from_slice(&size.to_le_bytes());
        entry.extend_from_slice(&size.to_le_bytes());
        entry.extend_from_slice(&name_len.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u16.to_le_bytes());
        entry.extend_from_slice(&0u32.to_le_bytes());
        entry.extend_from_slice(&offset.to_le_bytes());
        entry.extend_from_slice(name.as_bytes());
        offset = offset
            .checked_add(u32::try_from(local.len()).map_err(|_| AppError::TooLarge)?)
            .ok_or(AppError::TooLarge)?;
        locals.extend_from_slice(&local);
        central.extend_from_slice(&entry);
    }
    let cd_offset = u32::try_from(locals.len()).map_err(|_| AppError::TooLarge)?;
    let cd_size = u32::try_from(central.len()).map_err(|_| AppError::TooLarge)?;
    let count = u16::try_from(files.len()).map_err(|_| AppError::TooLarge)?;
    let mut out = locals;
    out.extend_from_slice(&central);
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    Ok(out)
}

pub fn read_stored_zip(bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>, AppError> {
    if bytes.len() < 22
        || u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0; 4])) != 0x0403_4b50
    {
        return Err(AppError::Validation("photo archive is not a zip".into()));
    }
    let mut files = Vec::new();
    let mut cursor = 0usize;
    let mut total = 0u64;
    while cursor + 4 <= bytes.len()
        && u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap_or([0; 4])) == 0x0403_4b50
    {
        if files.len() >= MAX_ENTRIES || cursor + 30 > bytes.len() {
            return Err(AppError::Validation("photo archive is invalid".into()));
        }
        let method =
            u16::from_le_bytes(bytes[cursor + 8..cursor + 10].try_into().unwrap_or([0; 2]));
        let size = u32::from_le_bytes(bytes[cursor + 18..cursor + 22].try_into().unwrap_or([0; 4]))
            as usize;
        let name_len =
            u16::from_le_bytes(bytes[cursor + 26..cursor + 28].try_into().unwrap_or([0; 2]))
                as usize;
        let extra_len =
            u16::from_le_bytes(bytes[cursor + 28..cursor + 30].try_into().unwrap_or([0; 2]))
                as usize;
        let name_at = cursor + 30;
        let data_at = name_at + name_len + extra_len;
        let data_end = data_at + size;
        if method != 0 || data_end > bytes.len() {
            return Err(AppError::Validation("photo archive is invalid".into()));
        }
        let name = std::str::from_utf8(&bytes[name_at..name_at + name_len])
            .map_err(|_| AppError::Validation("photo archive name is invalid".into()))?;
        if !valid_name(name) {
            return Err(AppError::Validation("photo archive name is invalid".into()));
        }
        total = total.saturating_add(size as u64);
        if total > MAX_UNCOMPRESSED {
            return Err(AppError::TooLarge);
        }
        let data = bytes[data_at..data_end].to_vec();
        if data.len() < 3 || data[0] != 0xFF || data[1] != 0xD8 || data[2] != 0xFF {
            return Err(AppError::Validation("photo archive contains a non-jpeg".into()));
        }
        files.push((name.to_owned(), data));
        cursor = data_end;
    }
    if files.is_empty() {
        return Err(AppError::Validation("photo archive is empty".into()));
    }
    Ok(files)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_' || byte == b'-'
        })
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_zip_round_trips_jpegs() {
        let first = vec![0xFF, 0xD8, 0xFF, 0xD9];
        let second = vec![0xFF, 0xD8, 0xFF, 0x00, 0xD9];
        let bytes = write_stored_zip(&[
            ("01.jpg".into(), first.clone()),
            ("02.jpg".into(), second.clone()),
        ])
        .expect("zip");
        let files = read_stored_zip(&bytes).expect("read");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].1, first);
        assert_eq!(files[1].1, second);
    }
}
