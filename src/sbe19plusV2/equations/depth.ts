import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


export function depth(df: Table, casts: Object[], type: string = "salt water"): Table {
    /* 
        Function to calculate the Depth (m)
    */
    let depth = new Float32Array(df.length);
    let p: any = null, x: number = null, gr: number = null, filteredCasts: Object = null;

    df.scan((idx) => {
        if (type === "fresh water") { 
            depth[idx] = p(idx) * 1.019716;
        } else {
            filteredCasts = casts.filter(x => {
                return idx >= x['startNum'] - 1 && idx < x['endNum'];
            })
            x = (math.sin(filteredCasts[0]["latitude"] / 57.29578)) ** 2;
            gr = 9.780318 * (1.0 + (5.2788e-3 + 2.36e-5 * x) * x) + 1.092e-6 * p(idx);
            depth[idx] = (((-1.82e-15 * p(idx) + 2.279e-10) * p(idx) - 2.2512e-5) * p(idx) + 9.72659) * p(idx);
            if (gr !== null) 
                depth[idx] = depth[idx] / gr;
        }
    }, (batch) => {
        p = col("Pressure (dbars)").bind(batch);
    });
    let newCol: string = "Depth (m)";
    df = df.assign(Table.new([Float32Vector.from(depth)], [newCol]));
    return df;
}


function test_depth() {

    
}