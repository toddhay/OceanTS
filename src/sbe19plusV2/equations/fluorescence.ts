import * as math from 'mathjs';
import { Float32Vector, Float64Vector, Table, Dictionary } from 'apache-arrow';
import { col } from 'apache-arrow/compute/predicate';
import * as assert from 'assert';


export function fluorescence(df: Table): Table {
    /*
        Fluorescence reference:  SBE 19plusV2 manual, p. 57, Fluormeter Calibration Coefficients


    */
    let Cfn: number = null, A1: number = null, A2: number = null, B: number = null;
    let Fn: number = null, prod: number = null, Chl: number = null;

    let V: number = null, PAR: number = null;

    Fn = Cfn * (10 ^ V);
    prod = A1 * Fn / (A2 + PAR);
    Chl = Fn / (B * PAR)

    return df;
}


function test_fluorescence() {

    
}