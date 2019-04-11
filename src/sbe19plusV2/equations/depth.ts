import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


export function depth(df: Table, latitude: number, pressure: number): Table {
    /* 
        Function to calculate the Depth (m)
    */

    let depth = new Float32Array(df.length);
    let v: any = null, p: any = null;

    df.scan((idx) => {
        depth[idx] = 1;
    }, (batch) => {
        v = col(colName).bind(batch);
        p = col("Pressure (dbars)").bind(batch);
    });
    let newCol: string = "Depth (m)";
    df = df.assign(Table.new([Float32Vector.from(depth)], [newCol]));
    return df;
}


function test_depth() {

    
}