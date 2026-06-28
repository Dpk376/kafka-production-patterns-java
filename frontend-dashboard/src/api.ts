export const API_BASE_URL = 'http://localhost:8084/api/load';

export async function generateTraffic(count: number = 10): Promise<string> {
  try {
    const response = await fetch(`${API_BASE_URL}/generate?count=${count}`, {
      method: 'POST',
      headers: {
        'Accept': 'text/plain'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to generate traffic: ${response.statusText}`);
    }
    
    return await response.text();
  } catch (error) {
    console.error('Error generating traffic:', error);
    throw error;
  }
}

export async function sendPoisonPill(): Promise<string> {
  try {
    const response = await fetch(`${API_BASE_URL}/poison`, {
      method: 'POST',
      headers: {
        'Accept': 'text/plain'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to send poison pill: ${response.statusText}`);
    }
    
    return await response.text();
  } catch (error) {
    console.error('Error sending poison pill:', error);
    throw error;
  }
}
