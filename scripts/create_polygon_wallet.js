const { ethers } = require('ethers');
const fs = require('fs');
const path = require('path');

// Generate a new wallet
const wallet = ethers.Wallet.createRandom();

console.log("=== Chronicle Polygon Wallet ===");
console.log("");
console.log("Address:", wallet.address);
console.log("Public Key:", wallet.publicKey);
console.log("");
console.log("IMPORTANT: Save this private key securely!");
console.log("Private Key:", wallet.privateKey);
console.log("");

// Save to file (private)
const walletData = {
  address: wallet.address,
  privateKey: wallet.privateKey,
  publicKey: wallet.publicKey,
  createdAt: new Date().toISOString(),
  purpose: "Chronicle Polymarket predictions"
};

const walletPath = path.join(process.env.HOME, '.homeforge-chronicle', 'polygon_wallet.json');
fs.writeFileSync(walletPath, JSON.stringify(walletData, null, 2), { mode: 0o600 });
console.log("Wallet saved to:", walletPath);
console.log("");
console.log("Next steps:");
console.log("1. Send Polygon USDC to:", wallet.address);
console.log("2. Send small amount of MATIC for gas");
console.log("3. Run the execution script to place orders");
