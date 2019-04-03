import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


export function psia2dbar(p: number) {
    // Function to convert pressure in psia to dbar
    // p - pressure in psia
    return (p - 14.7) * 0.689476
}

export function pressure(df: Table, colName: string, colName2: string, c: Object): any {
    /*
        Pressure calcuation using A/D counts for Seabird 19plusV2

        Sample calibration coefficients for pressure calculation:

        {"PressureSensor":{"SerialNumber":5048,"CalibrationDate":"03-Mar-16","PA0":-7.76405289,
        "PA1":0.0156780081,"PA2":-6.48466876e-10,"PTEMPA0":-63.7429865,"PTEMPA1":51.7986913,
        "PTEMPA2":-0.226510802,"PTCA0":524405.088,"PTCA1":-22.4344595,"PTCA2":0.0108628644,
        "PTCB0":24.74475,"PTCB1":-0.00245,"PTCB2":0,"Offset":0}}
    */
    let p = new Float32Array(df.length);
    let counts: any = null, voltages: any = null;
    let y: number = null, t: number = null, x: number = null, n: number = null, pTemp: number = null;
    df.scan((idx) =>{
        y = voltages(idx);
        // if (idx >= df.length - 3) console.info(`y = ${y}`);
        t = c["PTEMPA0"] + c["PTEMPA1"] * y + c["PTEMPA2"] * (y ** 2);
        x = counts(idx) - c["PTCA0"] - c["PTCA1"] * t - c["PTCA2"] * (t ** 2);
        n = x * c["PTCB0"] / (c["PTCB0"] + c["PTCB1"] * t + c["PTCB2"] * (t ** 2))
        pTemp = c["PA0"] + c["PA1"] * n + c["PA2"] * (n ** 2);
        // p[idx] = pTemp;
        p[idx] = psia2dbar(pTemp);
    }, (batch) => {
        counts = col(colName).bind(batch);
        voltages = col(colName2).bind(batch);
    });
    let newCol: string = "Pressure (dbars)";
    df = df.assign(Table.new([Float32Vector.from(p)], [newCol]));
    return df;
}

function test_pressure() {

    let c = {
        "PA0": -7.764053e+000,
        "PA1": 1.567801e-002,
        "PA2": -6.484669e-010,
        "PTEMPA0": -6.374299e+001,
        "PTEMPA1": 5.179869e+001,
        "PTEMPA2": -2.265108e-001,    
        "PTCA0": 5.244051e+005,
        "PTCA1": -2.243446e+001,
        "PTCA2": 1.086286e-002,
        "PTCB0": 2.474475e+001,
        "PTCB1": -2.450000e-003,
        "PTCB2": 0.000000e+000 
    }
    let counts = new Float32Array(
        [525336.8, 589897.9, 654847.6, 720155.1, 785813.8, 851830.2, 785809.9,
            720174.8, 654847.3, 589895.3, 525323.6]);
    let voltages = new Float32Array(counts.length).fill(1.7);
    let colName = "Pressure A/D Counts";
    let colName2 = "Pressure Temperature Compensation Voltage";
    let df = Table.new([Float32Vector.from(counts),
                        Float32Vector.from(voltages)], 
                        [colName, colName2]);
    df = pressure(df, colName, colName2, c);
    let pressureArray = df.getColumn('Pressure (dbars)').toArray().slice(-3);
    // console.info(`pressure: ${pressureArray}`);

}

// test_pressure();