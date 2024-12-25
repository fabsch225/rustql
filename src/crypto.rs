use std::time::{SystemTime, UNIX_EPOCH};

pub fn generate_random_hash(length: usize) -> String {
    let mut random_bytes = Vec::new();
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    for _ in 0..length {
        // Simple linear congruential generator (LCG) for pseudo-randomness
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        random_bytes.push((seed >> 32) as u8);
    }

    // Convert bytes to hexadecimal
    random_bytes.iter().map(|b| format!("{:02x}", b)).collect()
}