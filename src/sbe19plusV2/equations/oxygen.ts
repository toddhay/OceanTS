import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';

function umoles_per_l2ml_per_l(x: number): number {
    /*
        Function to convert Oxygen in umoles/l to ml/l

        x: number - umoles/l of oxygen

        Convert using the molar volume of oxgygen as 22.3916 L/mol at standard
        temperature and pressure (0 degC, 1 atmosphere) (per Garcia & Gordon 1992)

        References:
        https://www.oceanbestpractices.net/bitstream/handle/11329/417/56281.pdf?sequence=1&isAllowed=y

        OR

        p. 12, Section 5, Unit conversion of oxygen
        http://www.argodatamgt.org/content/download/2928/21973/file/

    */
   return x / 44.6596;
}

function oxygenSolubility(salinity: number, temperature: number): number {
    /*
    Method for calculating the Oxygen Solubility per Garcia & Gordon (1992), as discussed
    in the:
    
    Seabird Application Note 64, SBE 43 Dissolved Oxygen Sensor - Background Information, Deployment
        Recommendations, and Cleaning and Storage, p. 8
    https://www.seabird.com/asset-get.download.jsa?id=54627861706

    OR

    Seabird Data Processing Manual revision 7.26.8, p. 158, accessible here:
    https://www.seabird.com/asset-get.download.jsa?code=251446
    
    */

    // Define constants
    let A0 = 2.00907, A1 = 3.22014, A2 = 4.0501, A3 = 4.94457, A4 = -0.256847, A5 = 3.88767;
    let B0 = -0.00624523, B1 = -0.00737614, B2 = -0.010341, B3 = -0.00817083;
    let C0 = -0.000000488682;

    let oxySol: number = null, Ts: number = null;

    Ts = Math.log((298.15 - temperature) / (273.15 + temperature));
    oxySol = Math.exp(A0 + A1*Ts + A2*Ts**2 + A3*Ts**3 + A4*Ts**4 + A5*Ts**5 +
        salinity * (B0 +B1*Ts + B2*Ts**2 + B3*Ts**3) + C0*salinity**2);

    return oxySol;

}

export function oxygen_sbe43(df: Table, colName: string, c: Object, scanRate: number = 4): Table {
    /*
        Calculate the SBE43 Oxygen (ml_per_l) per:

        Seabird SBE43 Calibration Worksheet (does not include tau + dvdt corrections)
        https://github.com/nwfsc-fram/OceanTS/blob/master/docs/SBE%2043%20O1505%2022Nov16.pdf
        
        OR

        Seabird Application Note 64-2, SBE 43 Dissolved Oxygen Sensor Calibration & Data Corrections
        (this includes the tau + dvdt corrections)
        https://www.seabird.com/asset-get.download.jsa?id=54627861704
    */
    
    // Get Coefficients
    if (c["Use2007Equation"] === 1)
        c = c["CalibrationCoefficients"][1];
    else
        c = c["CalibrationCoefficients"][0];    // ToDo I don't currently handle this scenario
                                                // This is for data prior to 2007

    let oxy = new Float32Array(df.length);
    let t: any = null, p: any = null, s: any = null, v: any = null;
    let tau: number = null, dvdt: number = null, oxySol: number = null, K: number = null;


    try {
        df.scan((idx) => {
            tau = c["Tau20"] * Math.exp( c["D1"] * p(idx) + c["D2"] * (t(idx) - 20));
            // dvdt = v(idx-1) ? (v(idx) - v(idx-1)) * scanRate : 0;
            dvdt = 0;   // ToDo - Comment out - this temporariliy disables the tau correction
            /* Per Seabird, dvdt = change in volts from the SBE43 over the change in time it takes
                                   the instrument to travel 2 decibars during the profile.  */
            oxySol = oxygenSolubility(s(idx), t(idx));
            K = t(idx) + 273.15;
            oxy[idx] = c["Soc"] * (v(idx) + c["offset"] + tau * dvdt) * oxySol *
                (1.0 + c["A"] * t(idx) + c["B"] * t(idx)**2 + c["C"] * t(idx)**3 ) * Math.exp( c["E"] * p(idx) / K);
        }, (batch) => {
            s = col("Salinity (psu)").bind(batch);
            t = col("Temperature (degC)").bind(batch);
            p = col("Pressure (dbars)").bind(batch);
            v = col(colName).bind(batch);
        });
        let newCol: string = "Oxygen (ml_per_l)";
        df = df.assign(Table.new([Float32Vector.from(oxy)], [newCol]));
    } catch(e) {
        console.log(e);
    }

    return df;  
}

export function oxygen_optode(df: Table, colName: string, c:Object): Table {
    /*
        Method to calculate the oxygen in ml/l units from the OPTODE Aanderaa sensor
    */
   let oxy = new Float32Array(df.length);
   let v: any = null;

   try {
       df.scan((idx) => {
           oxy[idx] = umoles_per_l2ml_per_l(v(idx));
       }, (batch) => {
           v = col(colName).bind(batch);
       });
       let newCol: string = "OPTODE Oxygen (ml_per_l)";
       df = df.assign(Table.new([Float32Vector.from(oxy)], [newCol]));
   } catch(e) {
       console.log(e);
   }
    return df
}


function test_oxygen() {

    let c = {
        "Use2007Equation": 1,
        "CalibrationCoefficients": [
            {},
            {
                "Soc": 0.4664,
                "offset": -0.5153,
                "Tau20": 1.49,
                "A": -3.9628e-003,
                "B": 1.8060e-004,
                "C": -2.3781e-006,
                "E": 0.036,
                "D1": 1.92634e-4,
                "D2": -4.64803e-2,
                "H1": -3.300000e-2,
                "H2": 5.00000e+3,
                "H3": 1.45000e+3  
            }
        ]
    };
    let voltages = [0.759, 0.836, 0.791, 0.903, 0.958, 0.991, 1.480, 1.641, 1.853, 2.012, 1.380, 2.141, 2.439,
            2.797, 2.170, 3.067, 3.250, 1.995];
    let temperatures = [2.00, 12.00, 6.00, 20.00, 26.00, 30.00, 6.00, 12.00, 20.00, 26.00, 2.00, 30.00, 12.00,
            20.00, 6.00, 26.00, 30.00, 2.00];
    let pressures = Array(voltages.length).fill(0.00);
    let salinities = Array(voltages.length).fill(0.00);
    let colName = "External Voltage 0";
    let df = Table.new([Float32Vector.from(voltages),
                        Float32Vector.from(temperatures),
                        Float32Vector.from(pressures),
                        Float32Vector.from(salinities)],
                        [colName, "Temperature (degC)", "Pressure (dbars)", "Salinity (psu)"]);
    df = oxygen_sbe43(df, colName, c);
    let oxygenArray = df.getColumn("Oxygen (ml_per_l)").toArray();
    let trueValues = [1.09, 1.10, 1.10, 1.12, 1.14, 1.15, 3.85, 3.86, 3.87, 3.87, 3.87,
        3.93, 6.59, 6.59, 6.60, 6.60, 6.61, 6.63];

    console.info(`\nOxygen (SBE43) Unit Test`);
    console.info('\tGround Truth\tCalculated Value\tDiff');
    trueValues.forEach((x, idx) => {
        console.info(`\t${x}\t\t${oxygenArray[idx].toFixed(2)}\t\t\t${(x - oxygenArray[idx]).toFixed(2)}`);
    });
}

// test_oxygen();