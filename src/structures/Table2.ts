import { Table, DateVector, Float32Vector, Utf8Vector, Int32Vector } from "apache-arrow";
import { col } from "apache-arrow/compute/predicate";
import { logger } from "../logger";
import * as papa from 'papaparse';


export function sliceByTimeRange(data: Table, colName: string, startTime: any, endTime: any): Table {
    return data.filter(col(colName).gt(startTime).and(col(colName).lt(endTime)));
}

export function slice(data: Table, start: number, end: number): Table {
    /*
        Method to return a slice of the arrow table from start to end
    */
    let headers = data.schema.fields.map((d: any) => d.name);
    let types = data.schema.fields.map((d: any) => d.type);
    let typeMapping = {
        'Float32': Float32Vector,
        'Date64<MILLISECOND>': DateVector,
        'Int32': Int32Vector,
        'Utf8': Utf8Vector
    }
    let arrays = [], tempArr = [], header: string = '';
    for (let i in headers) {
        header = headers[i];
        tempArr = typeMapping[types[i]].from(data.getColumn(header).toArray().slice(start, end));
        arrays.push(tempArr)
    }
   return Table.new(arrays, headers);
}

export function median(data: Table, colName: string): number {
    /*
    Method to determine the mode of a given column
    */
    let col = data.getColumn(colName);
    let sortedCol = col.toArray().sort();
    let middle = Math.floor(sortedCol.length / 2);
    return sortedCol[middle];
}

export function medianThreshold(data: Table, colName: string, median: number, 
    threshold: number, dropNull: boolean = false): any {
    /*
        Method to discard values outside of a given threshold from the median
    */
    let v: any = null, result = new Float32Array(data.length);
    let diff: number = Math.abs(median * threshold);
    let upperBound: number = median + diff;
    let lowerBound: number = median - diff;
    data.scan((idx) => {
        if (lowerBound <= v(idx) && v(idx) <= upperBound) {
            result[idx] = v(idx);
        } else {
            if (!dropNull) result[idx] = null;
        }
    }, (batch) => {
        v = col(colName).bind(batch);
    });
    return result;
}

export function sum(previousValue: number, currentValue: number) {
    return previousValue + currentValue;
}

export function splitHauls(data: Table, haulColName: string = "HaulID"): Object {
    /*
        Method to split a given arrow table into multiple tables based on 
            the haulColName

        data: Apache Arrow table
        haulColName: string - 
    */

    // Find the unique haul IDs in the data table
    // let haulIDs = data.getColumn(haulColName).toArray();
    // let haulsList = [...new Set(haulIDs)];
    let haulsList = [], v: any;
    data.scan((idx) => {
        if (!(haulsList.includes(v(idx)))) {
            haulsList.push(v(idx));
        }
    }, (batch) => {
        v = col(haulColName).bind(batch);
    });

    // Slice the data by haulID
    let haulID: string = '', haulData: any = null, haulsDict = {};
    for (let i in haulsList) {
        haulID = haulsList[i];
        haulData = data.filter(col(haulColName).eq(haulID));
        haulsDict[haulID] = haulData;
    }

    return haulsDict;
}

export function parseFile (rawFile: any): Promise<Object> {
    return new Promise(resolve => {
        papa.parse(rawFile, {
            delimiter: ",",
            header: true,
            dynamicTyping: true,
            skipEmptyLines: true,
            complete: (results: any) => {
                resolve(results);
            }
        });
    });
};

export async function csvToTable(input: string | Buffer): Promise<Table> {
    /*
        Method to create an Apache Arrow table from a csv file

        Inputs:
        fileName: string - the full path to the csv file
    */
    let utf8Strings: string[] = ["HaulID", "trawl_id", "vessel"];
    let dateStrings: string[] = ["DateTime (ISO8601)", "tow_end_timestamp", "tow_start_timestamp"];
    let intStrings: string[] = ["Year", "Month", "Day"];

    // let rawFile = readFileSync(fileName, "utf8");
    let parsedFile: any = await parseFile(input);
    let data = parsedFile.data;
    let fields = parsedFile.meta.fields;
    let field: string = '';

    let dataArrays = [], arr = [];
    for (let x in fields) {
        field = fields[x];
        arr = data.map((y: any) => y[field]);
        if (utf8Strings.includes(field)) {
            dataArrays.push(Utf8Vector.from(arr.map((y: any) => y.toString())));
        } else if (dateStrings.includes(field)) {
            dataArrays.push(DateVector.from(arr));
        } else if (intStrings.includes(field)) {
            dataArrays.push(Int32Vector.from(arr.map((y: any) => parseInt(y))));
        } else {
            dataArrays.push(Float32Vector.from(arr.map((y: any) => parseFloat(y))));;
        }
    }
    // Create arrow table data structure from the vectors + fields
    return new Promise(resolve => {
        resolve(Table.new(dataArrays, fields));
    })
}

export class Table2 extends Table {

    constructor(...args: any[]) { super(...args); };

    static getInstance(arrays: any[], headers: string[]): Table2 {
        return Table2.new(arrays, headers);
        // return new this;
   }

    public slice(start: number, end: number): Table2 {
        /*
            Method to return a slice of the arrow table from start to end
        */
        let headers = this.schema.fields.map((d: any) => d.name);
        let types = this.schema.fields.map((d: any) => d.type);
        let typeMapping = {
            'Float32': Float32Vector,
            'Date64<MILLISECOND>': DateVector,
            'Int32': Int32Vector,
            'Utf8': Utf8Vector
        }
        let arrays = [], tempArr = [], header: string = '';
        for (let i in headers) {
            header = headers[i];
            tempArr = typeMapping[types[i]].from(this.getColumn(header).toArray().slice(start, end));
            arrays.push(tempArr)
        }
        // return this.new(arrays, headers);
       return Table2.getInstance(arrays, headers);
    //    return Table2.new(arrays, headers);
    }
    public mode(colName: string) {
        /*
            Method to determine the mode of a given column
        */
        let col = this.getColumn(colName);
        // logger.info(`type = ${col.ArrayType}`);
        let sortedCol = col.toArray().sort();
        let middle = Math.floor(sortedCol.length / 2);
        return sortedCol[middle];
    }
    public mean(size: number) {
        /*
            Method to return the running mean around a window of size size


        */
        return null;
    }
}