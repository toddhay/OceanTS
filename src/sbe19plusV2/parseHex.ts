import { createReadStream } from 'fs';
import * as stream from 'stream';
import { createInterface } from 'readline';
import { Table, FloatVector, Dictionary } from 'apache-arrow';
import * as moment from 'moment';
import * as math from 'mathjs';
import { hex2dec, counts2frequency } from '../utilities';
import { convertResults } from './convertResults';


export function parseHex(hexFile: string, instrument: Dictionary, coefficients: Dictionary[]) {
    /*
    hexFile: string - full path to a Seabird 19plusV2 hex file
    */

    let dataStartLine: number = -1, lineNum: number = -1;
    let serialNumber: any = null;
    let endDateTime: any = null;
    let samples: number = null;
    let parts: any = null;
    let subparts: any = null;
    let pressureSensor = {}, extraSensors = {}, voltages = {}, casts = {};
    let castNum: string, castStartDate: string, castStartEnd: any, castStartNum: number, castEndNum: number, castAvg: number;

    const parsingRules = [
        {"sensor": "Temperature", "variable": "Temperature A/D Counts", "size": 6, "data": null, "isADCount": true, "operations": null},
        {"sensor": "Conductivity", "variable": "Conductivity Frequency", "size": 6, "data": null, 
            "operations": [{"op": math.divide, "value": 256}]},
        {"sensor": "Pressure", "variable": "Pressure A/D Counts", "size": 6, "data": null, "isADCount": true, "operations": null},
        {"sensor": "Pressure", "variable": "Pressure Temperature Compensation Voltage", "size": 4, "data": null, 
            "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 0", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 1", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 2", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 3", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 4", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "Voltage", "variable": "External Voltage 5", "size": 4, "data": null, "operations": [{"op": math.divide, "value": 13107}]},
        {"sensor": "SBE38", "variable": "SBE38 Temperature", "size": 6, "data": null, "operations": 
            [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},
        {"sensor": "WETLABS", "variable": "WETLABS Signal Counts", "size": 12, "data": null, "operations": null},
        {"sensor": "GasTensionDevice", "variable": "GTD Pressure", "size": 6, "data": null, 
            "operations": [{"op": math.divide, "value": 100000}]},
        {"sensor": "GasTensionDevice", "variable": "GTD Temperature", "size": 6, "data": null,
            "operations": [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},
        {"sensor": "OPTODE", "variable": "OPTODE Oxygen", "size": 6, "data": null,
            "operations": [{"op": math.divide, "value": 10000}, {"op": math.subtract, "value": 10}]},
        {"sensor": "SBE63", "variable": "SBE63 Oxygen Phase", "size": 6, "data": null,
            "operations": [{"op": math.divide, "value": 100000}, {"op": math.subtract, "value": 10}]},                              
        {"sensor": "SBE63", "variable": "SBE63 Oxygen Temperature Voltage", "size": 6, "data": null, 
            "operations": [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 1}]},
        {"sensor": "SeaFET", "variable": "SeaFET Internal Reference Cell Voltage", "size": 7, "data": null,
            "operations": [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 8}]},
        {"sensor": "SeaFET", "variable": "SeaFET External Reference Cell Voltage", "size": 12, "data": null,
            "operations": [{"op": math.divide, "value": 1000000}, {"op": math.subtract, "value": 8}]},
        {"sensor": "Clock", "variable": "Time, Seconds since January 1, 2000", "size": 8, "data": null, "operations": null}
    ]
    let currentChar: number = 0, dataRow: number = 0, value: any = null, schema = [];
    let df = null;

    let start = moment();

    // const output = new stream.PassThrough({ objectMode: true });
    let output: any = null;

    const line_counter = ((i = 0) => () => ++i)();
    const lineReader = createInterface({ input: createReadStream(hexFile) });

    lineReader.on('line', (line, lineNum = line_counter()) => {

    // for await (const line of lineReader) {}

        // console.info(`${lineNum}: ${line}`);

        // if ((lineNum > dataStartLine) && (dataStartLine !== -1)) lineReader.close();

        if ((line.startsWith('* SBE 19plus V 2.5.2')) && (endDateTime === null)) {
            // Parse the ending date/time and serial number
            const lineParts = line.split("SERIAL NO.").map(s => s.trim());
            if (lineParts.length === 2) {
                serialNumber = lineParts[1].split(' ')[0];
                endDateTime = lineParts[1].replace(serialNumber, "").trim()
                console.info('endDateTime: ' + endDateTime);
            }
        }

        if (line.startsWith("* samples")) {
            parts = line.split(",").map(s => s.replace("*", "").trim());
            subparts = parts[0].split("=")
            samples = parseInt(subparts[1]);
        }

        // ToDo - Parse mode + pump delay

        // ToDo - Parse autorun + magnetic modee

        if (line.startsWith("* pressure sensor")) {
            // Parse pressure sensor information
            parts = line.split(",").map(s => s.replace("*", "").trim());
            parts.forEach((x: any) => {
                subparts = x.split("=").map(s => s.trim());
                if (subparts.length === 2) 
                    pressureSensor[subparts[0]] = subparts[1];
            })
        }

        if (line.startsWith("* SBE 38")) {
            // Parse for determining if extra sensors exist or not
            parts = line.split(",");
            parts.forEach((x: any) => {
                subparts = x.split("=").map(s => s.replace("*", "").trim());
                if (subparts.length === 2) {
                    extraSensors[subparts[0].replace(/ /g,'')] = subparts[1] === "yes" ? true : false;
                }
            })
        }

        if (line.startsWith("* Ext Volt")) {
            // Parse extra voltage data
            parts = line.split(",").map(s => s.replace("*", "").trim());
            parts.forEach((x: any) => {
                subparts = x.split("=").map(s => s.trim());
                if (subparts.length === 2) 
                    voltages[subparts[0].replace("Ext", "External").replace("Volt", "Voltage")] = 
                        subparts[1] === "yes" ? true : false;
            })
        }

        if (line.startsWith("* cast")) {
            // Parse casts
            parts = line.replace("* cast", "").trim().split(",");
            if (parts.length >= 1) {
                subparts = parts[0].split("samples").map(s => s.trim());
                castNum = subparts[0].split(" ")[0];
                castStartDate = subparts[0].replace(castNum, "").trim();
                castStartEnd = subparts[1].split("to").map(s => s.trim());
                castStartNum = parseInt(castStartEnd[0]);
                castEndNum = parseInt(castStartEnd[1]);
                castAvg = parseInt(parts[1].split("=").map(s => s.trim())[1]);
                casts[castNum] = {
                    "startDate": castStartDate,
                    "startNum": castStartNum,
                    "endNum": castEndNum,
                    "avg": castAvg
                }
            }
        }

        // ToDo - Parse Voltage Offsets

        if (line.startsWith('*END*')) {
            // Parse where the data lines start
            dataStartLine = lineNum + 1; 

            // console.info('sensors: ' + JSON.stringify(sensors));
            // console.info('extraSensors: ' + JSON.stringify(extraSensors));
            // console.info('voltages: '+ JSON.stringify(voltages));
            // console.info('casts: ' + JSON.stringify(casts));
            // console.info('dataStartLine: ' + dataStartLine);
        }

        if ((lineNum >= dataStartLine) && (dataStartLine !== -1)) {
            
            // Parse Data
            currentChar = 0;
            value = null;

            parsingRules.forEach(rule => {
                if (((rule.sensor.startsWith("Voltage")) && !(voltages[rule.variable])) ||
                    ((rule.sensor in extraSensors) && !(extraSensors[rule.sensor]))) {
                    return;
                }
                value = hex2dec(line.slice(currentChar, currentChar+rule.size));
                if ((rule.operations !== null) && !(isNaN(value))) {
                    rule.operations.forEach((operation: any) => {
                        // console.info(rule.variable);
                        // console.info('\tvalue before', line.slice(currentChar, currentChar+rule.size), 'convert to', value, operation.op.name, 'by', operation.value);
                        value = operation.op(value, operation.value);   // Perform the rule math operation on the value
                        // console.info('\tvalue after', value)
                    });
                }
                // else {
                //     console.info(rule.variable, line.slice(currentChar, currentChar+rule.size), 'convert to', value);
                // }
                // if (rule.isADCount) {
                //     value = counts2frequency(value);
                    // if (rule.variable.indexOf('A/D Counts') > -1) 
                    //     rule.variable = rule.variable.replace("A/D Counts", "Frequency");
                // }
                if ((value !== null) && !(isNaN(value))) {
                    if (lineNum === dataStartLine) {
                        schema.push(rule.variable)
                        rule.data = new Float32Array(samples);
                    }
                    rule.data[dataRow] = value;
                    currentChar += rule.size;
                }
            });
            dataRow += 1;
        }
        lineNum += 1;
    });

    lineReader.on('close', function() {

    // await once(lineReader, 'close');

        let end = moment();
        let duration = moment.duration(end.diff(start));
        let dataArrays = [];
        let tempArray: any = null;
        schema.forEach(x => {
            tempArray = parsingRules.find(y => y.variable === x);
            dataArrays.push(FloatVector.from(tempArray.data));
        })
        df = Table.new(dataArrays, schema);

        const colName = 'Temperature A/D Counts';
        console.log(`   name: ${df.getColumn(colName).name} 
        type: ${df.getColumn(colName).type}, 
        length: ${df.length}, 
        first item: ${df.getColumn(colName).get(0)}, 
        second to end ${df.getColumn(colName).get(df.count()-2)},
        end ${df.getColumn(colName).get(df.count()-1)}`);

        console.info(`schema: ${schema}`)
        console.log('Processing completed, ' + dataRow + ' records processed, elapsed time: ' + duration.asSeconds() + 's');
        // convertResults({"df": df, "casts": casts, "instrument": instrument, "sensors": sensors});
        convertResults(instrument, coefficients, casts, df);
    });
}
