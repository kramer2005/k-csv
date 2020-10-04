import { createReadStream, ReadStream } from "fs";
import parse from "csv-parse";
import * as stream from "stream";
import { stdout } from "process";

/**
 * Transforma o objeto recebido em uma string em formato JSON
 */
class ObjectToString extends stream.Transform {
  constructor(options: stream.TransformOptions = {}) {
    options.objectMode = true;
    super(options);
  }
  _transform(
    record: any,
    encoding: BufferEncoding,
    callback: stream.TransformCallback
  ): void {
    this.push(JSON.stringify(record) + "\n");
    callback();
  }
}

/**
 * Interface para um objeto vazio que pode receber propriedades
 */
interface emptyType {
  [key: string]: any;
}

/**
 * Retorna objeto com as colunas selecionadas
 */
class SelectColumns extends stream.Transform {
  columns: Array<string>;
  /**
   * @param columns - Colunas a serem selecionadas
   */
  constructor(columns: Array<string>, options: stream.TransformOptions = {}) {
    options.objectMode = true;
    super(options);
    this.columns = columns;
  }
  _transform(
    record: any,
    encoding: BufferEncoding,
    callback: stream.TransformCallback
  ): void {
    let ret: emptyType = {};
    this.columns.forEach((element) => {
      if (record[element]) ret[element] = record[element];
      else this.emit("error", new Error(`A COLUNA ${element} NÃO EXISTE`));
    });
    this.push(ret);
    callback();
  }
}

/**
 * Acha as linhas com determinado valor
 */
class FindRows extends stream.Transform {
  column: string;
  line: any;
  /**
   * @param column - Coluna selecionada
   * @param line   - Valor a ser encontrado
   */
  constructor(
    column: string,
    line: any,
    options: stream.TransformOptions = {}
  ) {
    options.objectMode = true;
    super(options);
    this.line = line;
    this.column = column;
  }
  _transform(
    record: any,
    encoding: BufferEncoding,
    callback: stream.TransformCallback
  ) {
    if (record[this.column] === this.line) this.push(record);
    callback();
  }
}

/**
 * Cria os objetos a partir de um objeto modelo JÁ INSTANCIADO
 */
class CreateObjects extends stream.Transform {
  obj: object;
  /**
   * @param obj - Um objeto base já instanciado
   */
  constructor(obj: object, options: stream.TransformOptions = {}) {
    options.objectMode = true;
    super(options);
    this.obj = obj;
  }
  _transform(
    record: any,
    encoding: BufferEncoding,
    callback: stream.TransformCallback
  ) {
    let keys: Array<string> = Object.keys(this.obj);
    let ret: emptyType = {};
    keys.forEach((element) => {
      if (!record[element])
        this.emit("error", new Error(`Chave ${element} não encontrada`));
      switch (typeof this.obj[element]) {
        case "string":
          ret[element] = record[element];
          break;
        case "number":
          ret[element] = Number(record[element]);
          break;
        case "object":
          if (this.obj[element] instanceof Date) {
            ret[element] = new Date(record[element]);
          }
      }
    });
    this.push(ret);
    callback();
  }
}

class model {
  static "#" = 0;
  static Nome = "";
  static Idade = 0;
}

const csvRead = parse({ columns: true, delimiter: "," });

createReadStream("data.csv")
  .pipe(csvRead)
  .pipe(new CreateObjects(model))
  .pipe(new ObjectToString())
  .pipe(stdout);
