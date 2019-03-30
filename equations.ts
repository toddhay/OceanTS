

export function pressure(p: number) {
    // Function to convert pressure in psia to dbar
    // p - pressure in psia
    return (p - 14.7) * 0.689476
}