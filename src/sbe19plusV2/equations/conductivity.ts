import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


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

    df.scan((idx) => {
        f = v(idx) / 1000.0;       // Convert frequency from Hz to kHz
        cond[idx] = (c["G"] + c["H"] * f ** 2 + c["I"] * f ** 3 + c["J"] * f ** 4) /
            (1 + c["CTcor"] * t(idx) + c["CPcor"] * p(idx) );
    }, (batch) => {
        v = col(colName).bind(batch);
        t = col("Temperature (degC)").bind(batch);
        p = col("Pressure (dbars)").bind(batch);
    });
    let newCol: string = "Conductivity (S_per_m)";
    df = df.assign(Table.new([Float32Vector.from(cond)], [newCol]));
    return df;  
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

    let colName = "Conductivity Frequency";
    let colName2 = "Temperature (degC)";
    let colName3 = "Pressure (dbars)";
    let df = Table.new([Float32Vector.from(frequencies),
                        Float32Vector.from(temperatures),
                        Float32Vector.from(pressures)], 
                        [colName, colName2, colName3]);
    df = conductivity(df, colName, c);
    let conductivityArray = df.getColumn('Conductivity (S_per_m)').toArray();
    let trueValues = [0.0000, 2.9683, 3.2746, 4.2540, 4.5983, 5.1550, 5.6755, 6.0470];

    console.info(`\nConductivity Unit Test`);
    console.info('\tGround Truth\tCalculated Value\tDiff');
    trueValues.forEach((x, idx) => {
        console.info(`\t${x}\t\t${conductivityArray[idx].toFixed(4)}\t\t\t${(x - conductivityArray[idx]).toFixed(4)}`);
    });
}

test_conductivity();
