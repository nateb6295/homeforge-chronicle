const { ClobClient } = require("@polymarket/clob-client");
const { ethers } = require("ethers");
const fs = require("fs");
const path = require("path");

// Load private key from BASE wallet
const keyPath = path.join(process.env.HOME, ".base-wallet", "private_key.txt");
const privateKey = fs.readFileSync(keyPath, "utf8").trim();

// Polygon RPC
const POLYGON_RPC = "https://polygon-rpc.com";
const CLOB_API = "https://clob.polymarket.com";

// Market details
const MARKET = {
  slug: "will-bitcoin-hit-1m-before-gta-vi-872",
  question: "Will bitcoin hit $1m before GTA VI?",
  noToken: "91863162118308663069733924043159186005106558783397508844234610341221325526200"
};

async function main() {
  console.log("=== Chronicle Polymarket Trade ===\n");

  // Create wallet
  const provider = new ethers.providers.JsonRpcProvider(POLYGON_RPC);
  const wallet = new ethers.Wallet(privateKey, provider);
  console.log("Wallet:", wallet.address);

  // Create CLOB client
  const client = new ClobClient(CLOB_API, 137, wallet);

  try {
    // Step 1: Derive API credentials (one-time per wallet)
    console.log("\n1. Deriving API credentials...");
    const apiCreds = await client.deriveApiKey();
    console.log("API Key:", apiCreds.key.substring(0, 20) + "...");

    // Step 2: Create new client with L2 auth
    const authClient = new ClobClient(CLOB_API, 137, wallet, apiCreds);

    // Step 3: Place order
    console.log("\n2. Placing order...");
    const order = await authClient.createOrder({
      tokenID: MARKET.noToken,
      price: 0.52,  // 52 cents for NO
      side: "BUY",
      size: 38.46,  // ~$20 worth at 52 cents
    });

    console.log("Order created:", JSON.stringify(order, null, 2));

    // Step 4: Submit order
    const result = await authClient.postOrder(order);
    console.log("\n3. Order submitted!");
    console.log("Result:", JSON.stringify(result, null, 2));

  } catch (error) {
    console.error("Error:", error.message);
    if (error.response) {
      console.error("Response:", error.response.data);
    }
  }
}

main();
