const BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

export class ApiError extends Error {
    constructor(public status: number, message: string) {
        super(message);
    }
}

export async function apiGet<T>(path: string, apiKey: string): Promise<T> {
    const response = await fetch(`${BASE_URL}${path}`, {
        headers: { Authorization: `Bearer ${apiKey}` },
    });

    if (!response.ok) {
        let detail = response.statusText;
        try {
            detail = (await response.json()).detail ?? detail;
        } catch {
            /* body wasn't JSON; keep statusText */
        }
        throw new ApiError(response.status, detail);
    }

    return (await response.json()) as T;
}