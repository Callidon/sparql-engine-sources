/* file: n3-store.ts
MIT License

Copyright (c) 2019-2020 Thomas Minier

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import { Graph, ExecutionContext, PipelineInput, Pipeline } from 'sparql-engine'
import { Algebra } from 'sparqljs'
import { Store, Quad, NamedNode, Literal } from 'n3'
import { stringToTerm, termToString } from 'rdf-string'

/**
 * A RDF Graph implemented with a N3.js store as backend
 * @author Thomas Minier
 */
export default class N3Graph extends Graph {
  private _store: Store

  constructor () {
    super()
    this._store = new Store()
  }

  insert (triple: Algebra.TripleObject): Promise<void> {
    return new Promise((resolve, reject) => {
      this._store.addQuad(
        stringToTerm(triple.subject) as NamedNode,
        stringToTerm(triple.predicate) as NamedNode,
        stringToTerm(triple.object) as Literal,
        undefined,
        () => resolve())
    })
  }

  delete (triple: Algebra.TripleObject): Promise<void> {
    return new Promise((resolve, reject) => {
      this._store.removeQuad(
        stringToTerm(triple.subject) as NamedNode,
        stringToTerm(triple.predicate) as NamedNode,
        stringToTerm(triple.object) as Literal,
        undefined,
        () => resolve())
    })
  }

  find (pattern: Algebra.TripleObject, context: ExecutionContext): PipelineInput<Algebra.TripleObject> {
    return Pipeline.getInstance().fromAsync(input => {
      const stream = this._store.match(
        stringToTerm(pattern.subject) as NamedNode,
        stringToTerm(pattern.predicate) as NamedNode,
        stringToTerm(pattern.object) as Literal,
        undefined
      )

      stream.on('error', err => input.error(err))
      stream.on('end', () => input.complete())
      stream.on('data', (quad: Quad) => {
        input.next({
          subject: termToString(quad.subject),
          predicate: termToString(quad.predicate),
          object: termToString(quad.object)
        })
      })
    })
  }

  clear (): Promise<void> {
    throw new Error("Method not implemented.")
  }
}
