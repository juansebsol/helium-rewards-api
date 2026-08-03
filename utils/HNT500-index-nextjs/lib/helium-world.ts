import { sha256 } from "js-sha256";
import bs58 from "bs58";

const HELIUM_WORLD_HOTSPOT =
  "https://world.helium.com/en/network/mobile/hotspot";

function sha256Bytes(data: Uint8Array): Uint8Array {
  return new Uint8Array(sha256.arrayBuffer(data));
}

function isBase58Check(decoded: Uint8Array): boolean {
  if (decoded.length < 5) return false;
  const payload = decoded.subarray(0, -4);
  const checksum = decoded.subarray(-4);
  const first = sha256Bytes(payload);
  const second = sha256Bytes(first);
  for (let i = 0; i < 4; i++) {
    if (checksum[i] !== second[i]) return false;
  }
  return true;
}

function base58checkEncodeRaw(payload: Uint8Array, version = 0): string {
  const versioned = new Uint8Array(1 + payload.length);
  versioned[0] = version;
  versioned.set(payload, 1);
  const first = sha256Bytes(versioned);
  const second = sha256Bytes(first);
  const out = new Uint8Array(versioned.length + 4);
  out.set(versioned, 0);
  out.set(second.subarray(0, 4), versioned.length);
  return bs58.encode(out);
}

/**
 * Normalize rewards/HNT500 device_ids into Helium explorer entity_key_str
 * (base58check v0), which Helium World accepts on the mobile hotspot route.
 */
export function toExplorerEntityKey(deviceId: string): string | null {
  try {
    const decoded = bs58.decode(deviceId);
    if (isBase58Check(decoded)) return deviceId;
    if (decoded.length >= 32 && decoded.length <= 48 && decoded[0] === 0x00) {
      return deviceId;
    }
    return base58checkEncodeRaw(decoded, 0);
  } catch {
    return null;
  }
}

export function heliumWorldHotspotUrl(deviceId: string): string | null {
  const key = toExplorerEntityKey(deviceId) || deviceId;
  if (!key) return null;
  return `${HELIUM_WORLD_HOTSPOT}/${encodeURIComponent(key)}`;
}
