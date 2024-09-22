import crypto from 'crypto'


// Utility function to generate a random string (used for nonces)
const generateRandomString = (length) => {
    return crypto.randomBytes(length).toString('hex');
  };
  
  // Utility function to encode data in Base64 URL format
  const base64url = (source) => {
    // Encode in base64
    let encoded = Buffer.from(source).toString('base64');
    // Replace characters according to base64url specifications
    encoded = encoded.replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
    return encoded;
  };
  
  // Utility function to decode Base64 URL format
  const base64urlDecode = (source) => {
    // Pad with '=' to make it a multiple of 4
    let encoded = source.replace(/-/g, '+').replace(/_/g, '/');
    const pad = encoded.length % 4;
    if (pad) {
      encoded += '='.repeat(4 - pad);
    }
    return Buffer.from(encoded, 'base64').toString('utf8');
  };
  
  // Function to encrypt data using AES-256-CBC
  const encryptData = (data, secret) => {
    const iv = crypto.randomBytes(16); // Initialization vector
    const cipher = crypto.createCipheriv('aes-256-cbc', crypto.scryptSync(secret, 'salt', 32), iv);
    let encrypted = cipher.update(data, 'utf8', 'base64');
    encrypted += cipher.final('base64');
    return `${base64url(iv.toString('base64'))}:${base64url(encrypted)}`;
  };
  
  // Function to decrypt data using AES-256-CBC
  const decryptData = (encryptedData, secret) => {
    const [iv, encrypted] = encryptedData.split(':').map(base64urlDecode);
    const decipher = crypto.createDecipheriv('aes-256-cbc', crypto.scryptSync(secret, 'salt', 32), Buffer.from(iv, 'base64'));
    let decrypted = decipher.update(encrypted, 'base64', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  };
  
  // Function to create a secure token
   const createSecureToken = (payload, secret) => {
    // Step 1: Add a nonce (unique identifier) to the payload
    payload.nonce = generateRandomString(8); // 8-byte nonce for replay protection
    payload.iat = Math.floor(Date.now() / 1000); // Issued at timestamp
  
    // Step 2: Encrypt the payload
    const encryptedPayload = encryptData(JSON.stringify(payload), secret);
  
    // Step 3: Create the signature using HMAC with SHA-256
    const signature = base64url(crypto.createHmac('sha256', secret).update(encryptedPayload).digest('base64'));
  
    // Step 4: Combine encrypted payload and signature
    const secureToken = `${encryptedPayload}|${signature}`;
  
    return secureToken;
  };
  
  // Function to validate a secure token
  const validateSecureToken = (token, secret) => {
    // Split the token into its parts
    const [encryptedPayload, signature] = token.split('|');
  
    if (!encryptedPayload || !signature) {
      return { valid: false, error: 'Invalid token format' };
    }
  
    // Recreate the signature from the encrypted payload
    const expectedSignature = base64url(crypto.createHmac('sha256', secret).update(encryptedPayload).digest('base64'));
  
    // Validate the token by comparing signatures
    if (signature !== expectedSignature) {
      return { valid: false, error: 'Invalid signature' };
    }
  
    // Decrypt the payload
    try {
      const decryptedPayload = JSON.parse(decryptData(encryptedPayload, secret));
  
      // Optional: Check for expiration (assuming exp field is a Unix timestamp)
      if (decryptedPayload.exp && decryptedPayload.exp < Math.floor(Date.now() / 1000)) {
        return { valid: false, error: 'Token expired' };
      }
  
      return { valid: true, payload: decryptedPayload };
    } catch (error) {
      return { valid: false, error: 'Decryption failed' };
    }
  };
  


export {createSecureToken, validateSecureToken}


