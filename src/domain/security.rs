use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Returns true when an address is safe to contact from this service.
/// Loopback, private, link-local, multicast, documentation, and cloud metadata
/// ranges are rejected. IPv4-mapped IPv6 is classified as the inner IPv4 address.
pub fn ip_is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_public(v4),
        IpAddr::V6(v6) => ipv6_is_public(v6),
    }
}

fn ipv4_is_public(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    let value = u32::from(ip);
    if ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
    {
        return false;
    }
    // Carrier-grade NAT and benchmarking ranges are not globally routable targets.
    let cgnat = (value & 0xffc0_0000) == 0x6440_0000;
    let benchmarking = (value & 0xfffe_0000) == 0xc612_0000;
    let protocol_assignments = octets[0] == 192 && octets[1] == 0 && octets[2] == 0;
    let reserved = octets[0] >= 240;
    !(cgnat || benchmarking || protocol_assignments || reserved)
}

fn ipv6_is_public(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return ipv4_is_public(v4);
    }
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }
    let segments = ip.segments();
    let unique_local = (segments[0] & 0xfe00) == 0xfc00;
    let link_local = (segments[0] & 0xffc0) == 0xfe80;
    let documentation = segments[0] == 0x2001 && segments[1] == 0x0db8;
    let discard = segments[0] == 0x0100 && segments[1] == 0x0000;
    !(unique_local || link_local || documentation || discard)
}

pub fn host_is_blocked_name(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host == "metadata.google.internal"
        || host.ends_with(".internal")
        || host == "instance-data"
        || host == "metadata"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_private_and_metadata_addresses() {
        let blocked = [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "100.64.0.1",
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "255.255.255.255",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
        ];
        for raw in blocked {
            let ip: IpAddr = raw.parse().unwrap();
            assert!(!ip_is_public(ip), "{raw} should be blocked");
        }
    }

    #[test]
    fn allows_public_addresses() {
        let allowed = ["1.1.1.1", "8.8.8.8", "9.9.9.9", "2606:4700:4700::1111"];
        for raw in allowed {
            let ip: IpAddr = raw.parse().unwrap();
            assert!(ip_is_public(ip), "{raw} should be public");
        }
    }

    #[test]
    fn blocks_metadata_hostnames() {
        assert!(host_is_blocked_name("localhost"));
        assert!(host_is_blocked_name("metadata.google.internal"));
        assert!(host_is_blocked_name("foo.internal"));
        assert!(!host_is_blocked_name("example.com"));
    }
}
