use fast_rsync::{SignatureOptions, diff, apply};

fn main() {
    let old_data = b"Hello, this is the original file data.";
    let new_data = b"Hello, this is the updated file data with more stuff.";
    
    // 1. Generate signature for old_data
    let sig_options = SignatureOptions {
        block_size: 10,
        crypto_hash_size: 4,
    };
    let sig = fast_rsync::Signature::calculate(old_data, sig_options);
    
    // 2. Generate delta from signature and new_data
    let mut delta = Vec::new();
    fast_rsync::diff(&sig.index(), new_data, &mut delta).unwrap();
    
    println!("Old len: {}, New len: {}, Delta len: {}", old_data.len(), new_data.len(), delta.len());
    
    // 3. Apply delta to old_data
    let mut out = Vec::new();
    fast_rsync::apply(old_data, &delta, &mut out).unwrap();
    
    println!("Out: {}", String::from_utf8(out).unwrap());
}
