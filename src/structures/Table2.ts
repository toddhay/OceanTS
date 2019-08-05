import { Table, DateVector, Float32Vector, Utf8Vector, Int32Vector } from "apache-arrow";
import { col } from "apache-arrow/compute/predicate";
import { logger } from "../logger";
import * as papa from 'papaparse';
import * as moment from 'moment';


export function sliceByTimeRange(data: Table, colName: string, startTime: moment.Moment, endTime: moment.Moment): Table {
    let v: any = null, dt: any = null, startIdx: number = -1, endIdx: number = -1, previousIdx: number = -1;
    let currentDateTime: moment.Moment = null, previousDateTime: moment.Moment = null;

    if (endTime < startTime) {
        logger.info(`endTime is before the startTime, please correct`);
        return data;
    }
    data.scan((idx) => {
        currentDateTime = moment(v(idx));
        // if (idx === 0 || idx === data.count() - 1) 
        // if (idx % 1000 === 0)
            // logger.info(`\t\tidx = ${idx} > currentDateTime: ${currentDateTime.format('HH:mm:ss')}`)
        // if (idx < 5) logger.info(`currentDateTime = ${currentDateTime}, previousDateTime = ${previousDateTime}`);
        // if (previousDateTime === null) previousDateTime = currentDateTime;
        if (startIdx === -1) {
            if (currentDateTime >= startTime) {
                startIdx = idx;
                // logger.info(`\t\tsetting: startIdx = ${startIdx}, time = ${currentDateTime.format('HH:mm:ss')}`);
            }
            // if (currentDateTime < startTime) {

            // } else if (currentDateTime === startTime) {
            //     startIdx = idx;
            // } else if (currentDateTime > startTime && previousDateTime !== null && previousDateTime <= startTime) {
            //     startIdx = previousIdx;
            // }
        }        

        // if (startIdx === -1 && previousDateTime !== null && previousDateTime < startTime && currentDateTime >= startTime) {
        //     startIdx = idx;
        // }
        if (endIdx === -1) {
            if (currentDateTime >= endTime) {
                endIdx = idx;
                // logger.info(`\t\tsetting: endIdx = ${endIdx}, time = ${currentDateTime.format('HH:mm:ss')}`);
            }
            // if (currentDateTime < endTime) {

            // } else if (currentDateTime === endTime) {
            //     endIdx = idx;
            // } else if (currentDateTime > endTime && previousDateTime !== null && previousDateTime <= endTime) {
            //     endIdx = previousIdx;
            // }
        }
        // if (endIdx === -1 && previousDateTime < endTime && currentDateTime >= endTime) {
        //     endIdx = idx;
        // }
        previousIdx = idx;
        previousDateTime = currentDateTime;
    }, (batch) => {
        v = col(colName).bind(batch);
    });
    logger.info(`\t\t\tstartIdx = ${startIdx}, endIdx = ${endIdx}`);
    return slice(data, startIdx, endIdx);
    // return data.filter(col(colName).gt(startTime).and(col(colName).lt(endTime)));
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

export function splitHauls(data: Table, haulsColName: string = "HaulID"): Object {
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
        v = col(haulsColName).bind(batch);
    });

    // Slice the data by haulID
    let haulID: string = '', haulsDict = {}, haulData: any = null;
    for (let i in haulsList) {
        haulID = haulsList[i];
        let startIdx: number = -1, endIdx: number = -1;
        data.filter(col(haulsColName).eq(haulID))
            .scan((idx) => {
                if (startIdx === -1) startIdx = idx;
                endIdx = idx;
            }, (batch) => {                
            })
        // logger.info(`\t\thaul = ${haulID}, startIdx = ${startIdx}, endIdx = ${endIdx}`);
        haulsDict[haulID] = {'startIdx': startIdx, 'endIdx': endIdx + 1};
        // haulData = data.filter(col(haulsColName).eq(haulID));
        // haulsDict[haulID] = haulData;
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
            dataArrays.push(Utf8Vector.from(arr.map((y: any) => y ? y.toString() : "")));
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