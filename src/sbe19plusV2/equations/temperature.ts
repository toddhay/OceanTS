import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


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

test_temperature();