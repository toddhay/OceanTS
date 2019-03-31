import * as math from 'mathjs';
import { Float32Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';

export function pressure_psia2dbar(p: number) {
    // Function to convert pressure in psia to dbar
    // p - pressure in psia
    return (p - 14.7) * 0.689476
}

export function mv(n: number): number {
    // mv - used as part of Seabird 19plusV2 Temperature calculation
    return (n - 524288) / 1.6e+007;
}

export function r(mv: number): number {
    // r - used as part of Seabird 19plusV2 Temperature calculation
    return (mv * 2.900e+009 + 1.024e+008) / (2.048e+004 - mv * 2.0e+005);
}

export function temperature(df: Table, colName: string, c: Object): any {
    /* 
        Calculate the temperature (degC) from temperature A/D counts
    */
    let t90 = new Float32Array(df.length);
    let v: any = null;
    let temp: number = null;
    df.scan((idx) =>{
        temp = mv(v(idx));
        temp = r(temp);
        t90[idx] = ( (1 / ( c['A0'] + 
                        (c['A1'] * Math.log(temp)) + 
                        (c['A2'] * (Math.log(temp) ** 2)) + 
                        (c['A3'] * (Math.log(temp) ** 3)) ) ) - 273.15) *
                        c['Slope'] + c['Offset']; 
    }, (batch) => {
        v = col(colName).bind(batch);
    });
    let newCol: string = "Temperature (degC)";
    df = df.assign(Table.new([Float32Vector.from(t90)], [newCol]));
    return df;
}

export function temp_test() {

    let c = {
        "A0": -1.763479e-004,
        "A1": 3.078888e-004,
        "A2": -4.437235e-006,
        "A3": 2.064047e-007,
        "Slope": 1,
        "Offset": 0
    };
    let adCounts = new Float32Array(
        [656478.3, 563058.7, 362118.7, 314480.2, 253432.7, 209526.4, 183998.5]);
    let colName = "Temperature A/D Counts";
    let df = Table.new([Float32Vector.from(adCounts)], [colName]);

    df = temperature(df, colName, c);
    console.info(`temp: ${df.getColumn('Temperature (degC)').toArray()}`)

}

temp_test();


