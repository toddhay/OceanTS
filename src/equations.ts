import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';

export function pressure_psia2dbar(p: number) {
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
        p[idx] = pressure_psia2dbar(pTemp);
    }, (batch) => {
        counts = col(colName).bind(batch);
        voltages = col(colName2).bind(batch);
    });
    let newCol: string = "Pressure (dbars)";
    df = df.assign(Table.new([Float32Vector.from(p)], [newCol]));
    return df;
}

export function temperature(df: Table, colName: string, c: Object): any {
    /* 
        Calculate the temperature (degC) from temperature A/D counts
    */
    let t90 = new Float32Array(df.length);
    let v: any = null;
    let temp: number = null;
    df.scan((idx) =>{
        temp = (v(idx) - 524288) / 1.6e+007;
        temp = (temp * 2.900e+009 + 1.024e+008) / (2.048e+004 - temp * 2.0e+005);
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

export function conductivity(df: Table, colName: string, c: Object): any {
    /* 
        Calculate the conductivity (S_per_m) from conductivity frequency

        Sample calibration coefficients:

        {"ConductivitySensor":{"SerialNumber":5048,"CalibrationDate":"05-Mar-16","UseG_J":1,
        "SeriesR":0,"CellConst":2000,"ConductivityType":0,"Coefficients":[
            {"A":0,"B":0,"C":0,"D":0,"M":0,"CPcor":-9.57e-8},
            {"G":-1.03944282,"H":0.162763827,"I":-0.00151623599,"J":0.000127404151,"CPcor":-9.57e-8,
                "CTcor":0.00000325,"WBOTC":0}],
        "Slope":1,"Offset":0}}
    */
    let cond = new Float32Array(df.length);
    let f: number = null;
    let v: any = null, t: any = null, p: any = null;

    if (c["UseG_J"] === 1)
        c = c["Coefficients"][1];
    else
        c = c["Coefficients"][0];   // ToDo - I do not currently handle this scenario

    df.scan((idx) =>{
        f = v(idx) / 1000.0;       // Convert to frequency in kHz
        cond[idx] =  (c["G"] + c["H"] * f ** 2 + c["I"] * f ** 3 + c["J"] * f ** 4) /
            (10 * (1 + c["CTcor"] * t(idx) + c["CPcor"] * p(idx) ));
    }, (batch) => {
        v = col(colName).bind(batch);
        t = col("Temperature (degC)").bind(batch);
        p = col("Pressure (dbars)").bind(batch);
    });
    let newCol: string = "Conductivity (S_per_m)";
    df = df.assign(Table.new([Float32Vector.from(cond)], [newCol]));
    return df;  
}

function test_temperature() {

    let c = {
        "A0": 1.231679e-003,
        "A1": 2.625697e-004,
        "A2": -1.890234e-007,
        "A3": 1.542035e-007,
        "Slope": 1,
        "Offset": 0
    };
    let adCounts = new Float32Array(
        [675144.889, 601930.644, 417997.356, 368087.000, 299977.133, 247872.489, 216297.333]);
    let correctOutputs = new Float32Array(
        [1.0000, 4.4999, 15.0002, 18.4999, 23.9999, 29.0000, 32.5000]
    );
    let colName = "Temperature A/D Counts";
    let df = Table.new([Float32Vector.from(adCounts)], [colName]);

    df = temperature(df, colName, c);
    console.info(`temp: ${df.getColumn('Temperature (degC)').toArray()}`)
    let outputs = df.getColumn('Temperature (degC)').toArray();
    let precision: number = 3;
    outputs.forEach(function (value, idx) {
        console.info(`${idx} > ${value.toFixed(4)} ==? ${correctOutputs[idx]}`);
        // assert(value.toFixed(precision) === correctOutputs[idx], `temperature unit test failed, ${value.toFixed(precision)} !== ${correctOutputs[idx]}`);
    });

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

function test_conductivity() {

    let c = {
        "UseG_J": 1,
        "Coefficients": [
            {},
            {"G": -1.050175e+000,
            "H": 1.459037e-001,
            "I": -2.198678e-004,
            "J": 3.889823e-005,
            "CTcor": 3.2500e-006,
            "CPcor": -9.5700e-008,  }            
        ]
  
    }
    let frequencies = new Float32Array(
        [2685.71, 5249.53, 5445.26, 6027.74, 6219.29, 6516.75, 6782.81, 6966.29]);
    let temperatures = new Float32Array(
        [22.0000, 1.0000, 4.5000, 15.0001, 18.4999, 24.0000, 29.0000, 32.5000]
    );
    let pressures = new Float32Array(frequencies.length).fill(0);

    let colName = "Conducitivity Frequency";
    let colName2 = "Temperature (degC)";
    let colName3 = "Pressure (dbars)";
    let df = Table.new([Float32Vector.from(frequencies),
                        Float32Vector.from(temperatures),
                        Float32Vector.from(pressures)], 
                        [colName, colName2, colName3]);
    df = conductivity(df, colName, c);
    let conductivityArray = df.getColumn('Conductivity (S_per_m)').toArray();
    console.info(`conductivity: ${conductivityArray}`);

}
// test_temperature();
// test_pressure();
test_conductivity();


