import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


export function depth(df: Table, latitudes: Object, casts: Object, type: string = "fresh water"): Table {
    /* 
        Function to calculate the Depth (m)

    if type == "fresh water":
        d = pressure * 1.019716

    else:
        x = (math.sin(latitude / 57.29578))**2
        gr = 9.780318 * (1.0 + (5.2788e-3 + 2.36e-5 * x) * x) + 1.092e-6 * pressure
        d = (((-1.82e-15 * pressure + 2.279e-10) * pressure - 2.2512e-5) * pressure + 9.72659) * pressure
        if gr:
            d /= gr

    return d


    */

    let depth = new Float32Array(df.length);
    let p: any = null, x: number = null, gr: number = null;

    df.scan((idx) => {
        // TODO Implement the proper depth calculation
        // x = (math.sin())
        depth[idx] = 1;
    }, (batch) => {
        p = col("Pressure (dbars)").bind(batch);
    });
    let newCol: string = "Depth (m)";
    df = df.assign(Table.new([Float32Vector.from(depth)], [newCol]));
    return df;
}


function test_depth() {

    
}