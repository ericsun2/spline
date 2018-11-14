/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export interface SearchRequest {
    readonly text: string
    readonly offset: number

    withOffset(offset: number): SearchRequest
}

export class PageRequest implements SearchRequest {
    constructor(public readonly text: string,
                public readonly asAtTime = Date.now(),
                public readonly offset: number = 0) {
    }

    public withOffset(offset: number): PageRequest {
        return offset == this.offset
            ? this
            : new PageRequest(this.text,
                this.asAtTime,
                offset)
    }
}

export class IntervalRequest implements SearchRequest {

    constructor(public readonly text: string,
                public readonly from: number,
                public readonly to: number) {}

    readonly offset: number = 0

    withOffset(offset: number): SearchRequest {
        return this;
    }

}
