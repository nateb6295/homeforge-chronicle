const { ethers } = require('ethers');

// Compressed public key from Chronicle's threshold ECDSA (secp256k1)
const compressedPubKey = '02338937ba49eda216d9d7cb4eab5a258a68945fedb28dca470a4f7c76b003a90a';

// ethers v5 API
const pubKey = ethers.utils.computePublicKey('0x' + compressedPubKey, false);
const address = ethers.utils.computeAddress(pubKey);

console.log("=== Chronicle EVM/Polygon Address ===");
console.log("");
console.log("Compressed Public Key:", compressedPubKey);
console.log("Uncompressed Public Key:", pubKey);
console.log("");
console.log("Polygon Address:", address);
console.log("");
console.log("This address can receive USDC on Polygon!");
console.log("");
console.log("Verify on PolygonScan:");
console.log("https://polygonscan.com/address/" + address);
