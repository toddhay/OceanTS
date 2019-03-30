
export function hex2dec(x: string): number {
    return parseInt(x, 16);
}


export function counts2frequency(counts: number): number {
    /* Function to convert Analog/Digital counts to a frequency
        This is used for temperature, pressure, and voltage A/D counts

        counts:  number with 6 digits, each 2 digit group represents a byte of data

        return:  frequency - number
    */

    if (counts.toString().length !== 6) {
        console.error('Counts number of digits is not 6: ' + counts);
        return NaN;
    }
    let countsStr = counts.toString();
    return parseInt(countsStr.slice(0,2)) * 256 +
            parseInt(countsStr.slice(2,4)) +
            parseInt(countsStr.slice(4,6)) / 256

}