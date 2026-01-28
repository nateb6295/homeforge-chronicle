const { ethers } = require("ethers");
const fs = require("fs");
const path = require("path");

// Load private key
const keyPath = path.join(process.env.HOME, ".base-wallet", "private_key.txt");
const privateKey = fs.readFileSync(keyPath, "utf8").trim();

// Constants
const POLYGON_RPC = "https://polygon-rpc.com";
const POLYGON_USDC = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";
const BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
const POLYGON_CHAIN_ID = 137;
const BASE_CHAIN_ID = 8453;

// Amount to bridge (leave some for gas)
const AMOUNT_USDC = 145; // Bridge $145, keep ~$5 buffer

// ERC20 ABI for approve
const ERC20_ABI = [
  "function approve(address spender, uint256 amount) returns (bool)",
  "function balanceOf(address) view returns (uint256)",
  "function allowance(address owner, address spender) view returns (uint256)"
];

// SpokePool depositV3 ABI
const SPOKE_POOL_ABI = [
  "function depositV3(address depositor, address recipient, address inputToken, address outputToken, uint256 inputAmount, uint256 outputAmount, uint256 destinationChainId, address exclusiveRelayer, uint32 quoteTimestamp, uint32 fillDeadline, uint32 exclusivityDeadline, bytes message) payable"
];

async function main() {
  console.log("=== Bridge Polygon → BASE via Across ===\n");

  // Explicitly set network to avoid detection issues
  const provider = new ethers.providers.JsonRpcProvider(POLYGON_RPC, {
    name: "polygon",
    chainId: 137
  });
  const wallet = new ethers.Wallet(privateKey, provider);
  console.log("Wallet:", wallet.address);

  const amountRaw = ethers.utils.parseUnits(AMOUNT_USDC.toString(), 6);
  console.log(`Bridging: $${AMOUNT_USDC} USDC\n`);

  // Step 1: Get quote from Across
  console.log("1. Getting bridge quote...");
  const quoteUrl = `https://app.across.to/api/suggested-fees?inputToken=${POLYGON_USDC}&outputToken=${BASE_USDC}&originChainId=${POLYGON_CHAIN_ID}&destinationChainId=${BASE_CHAIN_ID}&amount=${amountRaw.toString()}`;

  const quoteResp = await fetch(quoteUrl);
  const quote = await quoteResp.json();

  if (quote.error) {
    throw new Error(`Quote error: ${quote.error}`);
  }

  const spokePool = quote.spokePoolAddress;
  const outputAmount = quote.outputAmount;
  const fillDeadline = parseInt(quote.fillDeadline);
  const exclusiveRelayer = quote.exclusiveRelayer || ethers.constants.AddressZero;
  const exclusivityDeadline = parseInt(quote.exclusivityDeadline) || 0;
  const quoteTimestamp = parseInt(quote.timestamp);
  const totalFee = quote.totalRelayFee?.total || "0";

  const outputUsdc = parseFloat(outputAmount) / 1e6;
  const feeUsdc = parseFloat(totalFee) / 1e6;

  console.log(`   SpokePool: ${spokePool}`);
  console.log(`   Output: $${outputUsdc.toFixed(2)} USDC`);
  console.log(`   Fee: $${feeUsdc.toFixed(4)}`);
  console.log(`   Fill deadline: ${new Date(fillDeadline * 1000).toISOString()}`);

  // Step 2: Approve USDC
  console.log("\n2. Approving USDC...");

  // Test connection first
  console.log("   Testing connection...");
  const blockNum = await provider.getBlockNumber();
  console.log(`   Connected, block: ${blockNum}`);

  const usdc = new ethers.Contract(POLYGON_USDC, ERC20_ABI, wallet);
  console.log("   Checking allowance...");
  const currentAllowance = await usdc.allowance(wallet.address, spokePool);

  if (currentAllowance.lt(amountRaw)) {
    // Polygon gas is high right now - use dynamic pricing
    const gasPrice = await provider.getGasPrice();
    const maxPriorityFee = ethers.utils.parseUnits("50", "gwei");
    const maxFee = gasPrice.mul(2); // Double current gas price
    console.log(`   Gas price: ${ethers.utils.formatUnits(gasPrice, "gwei")} gwei`);

    const approveTx = await usdc.approve(spokePool, ethers.constants.MaxUint256, {
      maxPriorityFeePerGas: maxPriorityFee,
      maxFeePerGas: maxFee
    });
    console.log(`   Approval tx: ${approveTx.hash}`);
    await approveTx.wait();
    console.log("   Approved!");
  } else {
    console.log("   Already approved");
  }

  // Step 3: Execute deposit
  console.log("\n3. Executing bridge deposit...");
  const spokePoolContract = new ethers.Contract(spokePool, SPOKE_POOL_ABI, wallet);

  // Use dynamic gas pricing
  const gasPrice2 = await provider.getGasPrice();
  const maxPriorityFee2 = ethers.utils.parseUnits("50", "gwei");
  const maxFee2 = gasPrice2.mul(2);
  console.log(`   Gas price: ${ethers.utils.formatUnits(gasPrice2, "gwei")} gwei`);

  const depositTx = await spokePoolContract.depositV3(
    wallet.address,           // depositor
    wallet.address,           // recipient
    POLYGON_USDC,             // inputToken
    BASE_USDC,                // outputToken
    amountRaw,                // inputAmount
    outputAmount,             // outputAmount
    BASE_CHAIN_ID,            // destinationChainId
    exclusiveRelayer,         // exclusiveRelayer
    quoteTimestamp,           // quoteTimestamp
    fillDeadline,             // fillDeadline
    exclusivityDeadline,      // exclusivityDeadline
    "0x",                     // message (empty)
    { gasLimit: 300000, maxPriorityFeePerGas: maxPriorityFee2, maxFeePerGas: maxFee2 }
  );

  console.log(`   Deposit tx: ${depositTx.hash}`);
  const receipt = await depositTx.wait();
  console.log(`   Confirmed in block ${receipt.blockNumber}`);

  console.log("\n✅ Bridge initiated!");
  console.log(`   Track at: https://app.across.to/transactions?search=${depositTx.hash}`);
  console.log("   Funds should arrive on BASE in ~2-5 minutes");
}

main().catch(console.error);
