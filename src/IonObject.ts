/*!
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import {Reader} from "./IonReader";
import {Writer} from "./IonWriter";
import {IonType} from "./IonType";
import {IonTypes} from "./IonTypes";
import { _hasValue } from "./util";
import JSBI from 'jsbi';
import { Decimal } from "./IonDecimal";
import { Timestamp } from "./IonTimestamp";

function possibly<O,T>(convert: (option: O) => T): (value: O | T | null) => T | null {
    return (value: O | T | null) => {
        if(value === null) {
            return null;
         }
         try{
            return convert(value as O);
         } catch(e) {
            return value as T
         }
    };
}

export function infer(value:any): IonObject<any> {
    if(value === null){
        return new IonNull();
    }
    if(value instanceof AbstractIonObject){
        return value as AbstractIonObject<any>;
    }
    if(isNaN(value)){
        if (value instanceof JSBI){
            return new IonInt(value as JSBI);
        }
        if(value instanceof Decimal){
            return new IonDecimal(value as Decimal);
        }
        if(typeof value === 'string' || value instanceof String){
            return new IonString(value as string);
        }
        if(typeof value === 'boolean' || value instanceof Boolean){
            return new IonBoolean(value as boolean);
        }
        if(Array.isArray(value)) {
            return new IonList(value as any[]);
        }
        if(value instanceof Date){
            return new IonTimestamp(value as Date);
        } 
        if(value instanceof Timestamp){
            return new IonTimestamp(value as Timestamp);
        }
        return new IonStruct(value);
    }
    let num = value as number;
    return Number.isInteger(num) ? new IonInt(num) : new IonFloat(num);
}

function forType(type: IonType): AbstractIonObject<any> {
    switch(type){
        case IonTypes.NULL: return new IonNull();
        case IonTypes.BOOL: return new IonBoolean();
        case IonTypes.INT: return new IonInt();
        case IonTypes.FLOAT: return new IonFloat();
        case IonTypes.DECIMAL: return new IonDecimal();
        case IonTypes.TIMESTAMP: return new IonTimestamp();
        case IonTypes.SYMBOL: return new IonSymbol();
        case IonTypes.STRING: return new IonString();
        case IonTypes.CLOB: return new IonClob();
        case IonTypes.BLOB: return new IonBlob();
        case IonTypes.LIST: return new IonList();
        case IonTypes.SEXP: return new IonSExpression();
        case IonTypes.STRUCT: return new IonStruct();
        default: throw new Error(`Cannot create IonObject for ${type}`);
    }
}

export function value(reader: Reader): IonObject<any> {
    let type: IonType | null = this.reader.type();
    if (type === null) {
        throw new Error("Reader is not pointed at a value");
    }
    let obj:AbstractIonObject<any> = forType(type);
    return obj.read(reader);
}

export function values(reader: Reader) {
    return {
        *[Symbol.iterator] () {
            let type: IonType | null = reader.type();
            if (type === null) {
                type = reader.next();
            }
            while (type !== null) {
                let obj:AbstractIonObject<any> = forType(type);
                yield obj.read(reader);
                type = reader.next();
            }
        }
    };
}

/**
 * 
 */
export interface IonObject<T> {
    readonly type: IonType;

    annotations: string[];

    value: T;

    readonly json: string;

    write(writer: Writer): void;

    read(reader: Reader): IonObject<T>;
}

export abstract class AbstractIonObject<T> implements IonObject<T | null> {
    private _annotations: string[] = [];

    constructor(protected readonly _type: IonType, protected _value: T | null){
    }

    get value(): T | null {
        return this._value;
    }

    set value(value: T | null) {
        this._value = value;
    }

    get type(): IonType {
        return this._type;
    }
    
    get annotations(): string[] {
        return this._annotations;
    }

    set annotations(annotations: string[]) {
        this._annotations = [...annotations];
    }

    get json(): string {
        return `${this.value}`;
    }

    read(reader: Reader): AbstractIonObject<T> {
        if (reader.type() != this.type || (reader.type() == null && this.type == IonTypes.NULL)){
            throw new Error(`Expected ${this.type.name}, but read ${reader.type()!.name}`);
        }
        this.annotations = reader.annotations();
        if(reader.isNull()) {
            this.value = null;
        } else {
            this._read(reader);
        }
        return this;
    }

    protected abstract _read(reader: Reader): void;

    write(writer: Writer): void {
        writer.setAnnotations(this._annotations);
        if(this.value === null) {
            writer.writeNull(this.type);
        } else {
            this._write(writer);
        }
    }

    protected abstract _write(writer: Writer): void;
}

export class IonNull extends AbstractIonObject<null> {
    constructor() {
        super(IonTypes.NULL, null);
    }

    set value(value: null) {
        if(value !== null) {
            throw new Error("Cannot set non-null value to null type");
        }
    }

    /* never called in current implementation */
    protected _read(reader: Reader): void {
        if(!reader.isNull()){
            throw new Error("Cannot set non-null value to null type");
        }
    }

    /* never called in current implementation */
    protected _write(writer: Writer): void {
        writer.writeNull(this.type);
    }
}

export class IonBoolean extends AbstractIonObject<boolean> {
    constructor(value: boolean | null = null){
        super(IonTypes.BOOL, value);
    }

    asBoolean(): boolean {
        return this.value === null ? false : this.value;
    }

    protected _read(reader: Reader): void {
        this.value = reader.booleanValue();
    }

    protected _write(writer: Writer): void {
        writer.writeBoolean(this.value);
    }
}

export class IonInt extends AbstractIonObject<number | JSBI> {
    constructor(value: number | JSBI | null = null){
        super(IonTypes.INT, value);
    }

    asNumber(): number | null {
        return possibly(JSBI.toNumber)(this.value);
    }

    asBigInt(): JSBI | null {
        return possibly(JSBI.BigInt)(this.value);
    }
    
    protected _read(reader: Reader): void {
        this.value = reader.bigIntValue();
    }

    protected _write(writer: Writer): void {
        writer.writeInt(this.asBigInt());
    }
}

export class IonFloat extends AbstractIonObject<number> {
    constructor(value: number | null = null) {
        super(IonTypes.FLOAT, value);
    }

    protected _read(reader: Reader): void {
        this.value = reader.numberValue();
    }

    protected _write(writer: Writer): void {
        writer.writeFloat64(this.value);
    }
}

export class IonDecimal extends AbstractIonObject<number | Decimal> {
    constructor(value: number | Decimal | null = null) {
        super(IonTypes.DECIMAL, value);
    }

    asNumber(): number | null {
        return possibly(Decimal.prototype.numberValue)(this.value);
    }

    asDecimal(): Decimal | null {
        return possibly((d: number) => Decimal.parse(`${d}`))(this.value);
    }

    get json(): string {
        return this.value instanceof Decimal ? (this.value as Decimal).toString() : super.json;
    }

    protected _read(reader: Reader): void {
        this.value = reader.decimalValue();
    }    
    
    protected _write(writer: Writer): void {
        writer.writeDecimal(this.asDecimal());
    }
}

export class IonTimestamp extends AbstractIonObject<Date | Timestamp> {
    constructor(value: Date | Timestamp | null = null) {
        super(IonTypes.TIMESTAMP, value);
    }

    asDate(): Date | null {
        return possibly(Timestamp.prototype.getDate)(this.value);
    }

    asTimestamp(): Timestamp | null {
        return possibly((d: Date) => Timestamp._valueOf(d, 0))(this.value);
    }

    get json(): string {
        return this.value === null ? 'null' : `"${this.asDate()}\"`;
    }

    protected _read(reader: Reader): void {
        this.value = reader.timestampValue();
    }    
    
    protected _write(writer: Writer): void {
        writer.writeTimestamp(this.asTimestamp());
    }   
}

export class IonSymbol extends AbstractIonObject<string> {
    constructor(value: string | null = null) {
        super(IonTypes.SYMBOL, value);
    }

    get json(): string {
        return this.value === null ? 'null' : `"${this.value}\"`;
    }

    protected _read(reader: Reader): void {
        this.value = reader.stringValue();
    }    
    
    protected _write(writer: Writer): void {
        writer.writeSymbol(this.value);
    }
}

export class IonString extends AbstractIonObject<string> {
    constructor(value: string | null = null) {
        super(IonTypes.STRING, value);
    }

    get json(): string {
        return this.value === null ? 'null' : `"${this.value}\"`;
    }

    protected _read(reader: Reader): void {
        this.value = reader.stringValue();
    }    
    
    protected _write(writer: Writer): void {
        writer.writeString(this.value);
    }
}

declare const Buffer;
export class IonClob extends AbstractIonObject<Uint8Array> {
    constructor(value: Uint8Array | null = null) {
        super(IonTypes.CLOB, value);
    }

    get json(): string {
        return this.value === null ? 'null' : '"' + Buffer.from(this.value).toString('ascii') + '"';
    }

    protected _read(reader: Reader): void {
        this.value = reader.byteValue();
    }
    
    protected _write(writer: Writer): void {
        writer.writeClob(this.value);
    }
}

export class IonBlob extends AbstractIonObject<Uint8Array> {
    constructor(value: Uint8Array | null = null) {
        super(IonTypes.BLOB, value);
    }

    get json(): string {
        return this.value === null ? 'null' : '"' + Buffer.from(this.value).toString('base64') + '"';
    }

    protected _read(reader: Reader): void {
        this.value = reader.byteValue();
    }
    
    protected _write(writer: Writer): void {
        writer.writeBlob(this.value);
    }
}

export class IonList extends AbstractIonObject<any[]> {
    constructor(value: any[] | null = null, type: IonType = IonTypes.LIST) {
        super(type, value);
    }

    get json(): string {
        return this.value === null ? 'null' : '[' + this.value.map(v => infer(v).json).join() + ']';
    }

    protected _read(reader: Reader): void {
        reader.stepIn();
        this.value = [];
        for(let type = reader.next(); type !== null; type = reader.next()){
            let obj:AbstractIonObject<any> = forType(type);
            obj.read(reader);
            this.value.push(obj);
        }
        reader.stepOut();
    }
    
    protected _write(writer: Writer): void {
        writer.stepIn(this.type);
        for(let elm of this.value!){
            infer(elm).write(writer);
        }
        writer.stepOut();
    }
}

export class IonSExpression extends IonList {
    constructor(value: any[] | null = null) {
        super(value, IonTypes.SEXP);
    }
}

export class IonStruct extends AbstractIonObject<any> {
    constructor(value: any = null) {
        super(IonTypes.STRUCT, value);
    }

    get json(): string {
        return this.value === null ? 'null' : '{' + Object.keys(this.value).map(k => `"${k}":`+infer(this.value[k]).json).join() + '}';
    }

    protected _read(reader: Reader): void {
        reader.stepIn();
        this.value = {};
        for(let type = reader.next(); type !== null; type = reader.next()){
            var key:string;
            try{
                key = reader.fieldName()!;
            } catch(e) {
                if(e.message == 'Symbol ID zero is unsupported'){
                    key = '';
                } else {
                    throw e;
                }
            }
            let obj:AbstractIonObject<any> = forType(type);
            obj.read(reader);
            this.value[key] = obj;
        }
        reader.stepOut();
    }
    
    protected _write(writer: Writer): void {
        writer.stepIn(this.type);
        for(let key of Object.keys(this.value!)){
            writer.writeFieldName(key)
            infer(this.value![key]).write(writer);
        }
        writer.stepOut();
    }
}
