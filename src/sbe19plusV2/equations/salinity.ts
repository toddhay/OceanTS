import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


export function salinity(df: Table): Table {
    /*
        Practical Salinity (PSU) calcuation

        The df table must contain the following columns:
        - Temperature (degC)
        - Pressure (dbars)
        - Conductivity (S_per_m)
    */
   let s = new Float32Array(df.length);
   let c: any = null, t: any = null, p: any = null;

   let A1 = 2.070e-5, A2 = -6.370e-10, A3 = 3.989e-15;
   let B1 = 3.426e-2, B2 = 4.464e-4, B3 = 4.215e-1, B4 = -3.107e-3;
   let C0 = 6.766097e-1, C1 = 2.00564e-2, C2 = 1.104259e-4, C3 = -6.9698e-7, C4 = 1.0031e-9;
   let a = [0.0080, -0.1692, 25.3851, 14.0941, -7.0261, 2.7081];
   let b = [0.0005, -0.0056, -0.0066, -0.0375, 0.0636, -0.0144];

   let c_temp: number = null, sum1: number = null, sum2: number = null, temp: number = null;
   let R: number = null, val: number = null, RP: number = null, RT: number = null;

    df.scan((idx) => {
        if (c(idx) <= 0.0)
            s[idx] = 0.0;
        else {
            c_temp = c(idx) * 10.0;
            R = c_temp / 42.914;
            val = 1 + B1 * t(idx) + B2 * t(idx)**2 + B3 * R + B4 * R * t(idx);
            if (val) {
                RP = 1 + (p(idx) * (A1 + p(idx) * (A2 + p(idx) * A3))) / val;
            }
            val = RP * (C0 + (t(idx) * (C1 + t(idx) * (C2 + t(idx) * (C3 + t(idx) * C4)))));
            if (val) {
                RT = R / val;
            }
            if (RT <= 0.0) {
                RT = 0.000001; 
            }
            sum1 = sum2 = 0.0
            for (let i: number = 0; i<6; i++) {
                temp = RT**(i/2.0);
                sum1 += a[i] * temp;
                sum2 += b[i] * temp;
            }
            val = 1.0 + 0.0162 * (t(idx) - 15.0);
            if (val) {
                s[idx] = sum1 + sum2 * (t(idx) - 15.0) / val;
            } else {
                s[idx] = -99.0;
            }
        }
    }, (batch) => {
        t = col("Temperature (degC)").bind(batch);
        p = col("Pressure (dbars)").bind(batch);
        c = col("Conductivity (S_per_m)").bind(batch);
    });
    let newCol: string = "Salinity (psu)";
    df = df.assign(Table.new([Float32Vector.from(s)], [newCol]));
    return df;
}

function test_salinity() {

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
    let conductivities = new Float32Array(
        [0.0000,2.9683,3.2746,4.2540,4.5983,5.1550,5.6755,6.0470]);
    let temperatures = new Float32Array(
        [22.0000, 1.0000, 4.5000, 15.0001, 18.4999, 24.0000, 29.0000, 32.5000]
    );
    let pressures = new Float32Array(conductivities.length).fill(0);

    let colName = "Conductivity (S_per_m)";
    let colName2 = "Temperature (degC)";
    let colName3 = "Pressure (dbars)";
    let df = Table.new([Float32Vector.from(conductivities),
                        Float32Vector.from(temperatures),
                        Float32Vector.from(pressures)], 
                        [colName, colName2, colName3]);
    df = salinity(df);
    let salinityArray = df.getColumn('Salinity (psu)').toArray();
    let trueValues = [0.0000, 34.7173, 34.6977, 34.6560, 34.6472, 34.6375, 34.6321, 34.6292];

    console.info(`\nSalinity Unit Test`);
    console.info('\tGround Truth\tCalculated Value\t\tDiff');
    trueValues.forEach((x, idx) => {
        console.info(`\t${x}\t\t${salinityArray[idx]}\t\t${(x - salinityArray[idx]).toFixed(4)}`);
    });

}

test_salinity();
