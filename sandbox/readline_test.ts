import { createReadStream } from 'fs';
import { createInterface } from 'readline';

let files = ["C:/test/1.hex", "C:/test/2.hex", "C:/test/3.hex"];

function parse(file: string) {
    let rl = createInterface({ input: createReadStream(file) });
    rl.on('line', (line: string) => {
        // console.info(`${line}`);
    });
    rl.on('close', function() {
        console.info(`finished parse ${file}`);
    });
}

function process() {
    files.forEach(async x => {
        console.info(`process ${x}`);
        await parse(x);
    });    
}

async function parse2(file: string) {
    console.info(`process2 ${file}`);
    const rl = createInterface({ input: createReadStream(file) });
    for await (const line of rl[Symbol.asyncIterator]()) {
        // console.info(`${line}`);
    }
    console.info(`finished parse2 ${file}`);
}

async function process2() {

    files.forEach(async (x: any, idx: number) => {
        // if (idx === 1) process.exit(0);
        await parse2(x)
    });
}

// process();
process2();