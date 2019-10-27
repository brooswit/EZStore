const asyncly = require('asyncly');

const assert = require('assert');

const _ = require('lodash');
const redis = require("redis");
const lowDB = require('lowdb'); const FileSync = require('lowdb/adapters/FileSync');
const mongo = require('mongodb').MongoClient;


class EZStore {
    constructor(databaseName, defaults={}, {
        memDBEnabled  = null,
        redisEnabled  = null,
            redisUrl  = null,
        lowBDEnabled  = null,
            lowDBPath = 'db.json',
        mongoEnabled  = null,
            mongoUrl  = 'mongodb://localhost:27017'
    }) {
        if (memDBEnabled && redisEnabled) {
            memDBEnabled = false;
        }

        if (memDBEnabled) {
            this.memory = {};
            _.defaults(this.memory, defaults);
        }
        if (redisEnabled) {
            this.redis = redis.createClient({
                url: redisUrl,
                db: databaseName,
            });
            // todo: defaults
        }
        if (lowBDEnabled) {
            const adapter = new FileSync(lowDBPath);
            this.lowDB = lowDB(adapter);
            this.lowDB.defaults(defaults);
        }
        if (mongoEnabled) {
            this._mongoReadyPromise = asyncly((done) => {
                mongo.connect(mongoUrl, (err, mongoClient) => {
                    assert.equal(null, err);
                    this.mongo = mongoClient.db(databaseName)
                    done();
                });
            });
        }
    }


////////////////////////////////////////////////////////////////////////////////
    async get(collectionName, recordId, hashKey) {
        assert.notEqual(undefined, collectionName);
        assert.notEqual(undefined, recordId);
        assert.notEqual(undefined, hashKey);

        const hasMemDBPromise = this.hasMemDB(collectionm, recordId, hashKey);
        const hasRedisPromise = this.hasRedis(collectionm, recordId, hashKey);
        const hasLowDBPromise = this.hasLowDB(collectionm, recordId, hashKey);
        const hasMongoPromise = this.hasMongo(collectionm, recordId, hashKey);

        const value = undefined;

        if(await hasMemDBPromise){
            value = await this.getMemDB(collectionName, recordId, hashKey);
        } else if(await hasRedisPromise){
            value = await this.getRedis(collectionName, recordId, hashKey);
            await this.setMemDB(collectionName, recordId, hashKey);
        } else if(await hasLowDBPromise){
            value = await this.getLowDB(collectionName, recordId, hashKey);
            await Promise.all([
                this.setRedis(collectionName, recordId, hashKey),
                this.setMemDB(collectionName, recordId, hashKey)
            ]);
        } else if(await hasMongoPromise)){
            value = await this.getMongo(collectionName, recordId, hashKey);
            await Promise.all([
                this.setLowDB(collectionName, recordId, hashKey),
                this.setRedis(collectionName, recordId, hashKey),
                this.setMemDB(collectionName, recordId, hashKey)
            ]);
        }

        return value;
    }
    async getMemDB(collectionName, recordId, hashKey) {
        if (!this.memDBEnabled) return undefined;
        return _.get(this.memory, [collectionName, recordId, hashKey]);
    }
    async getRedis(collectionName, recordId, hashKey) {
        if (!this.redisEnabled) return undefined;

        return await asyncly(async (done) => {
            this.redis.hget(`${collectionName}:${recordId}`, hashKey, (err, value) => {
                assert.equal(null, err);
                done(value);
            });
        });
    }
    async getLowDB(collectionName, recordId, hashKey) {
        if (!this.lowDBEnabled) return undefined;
        return this.lowDB.get(this.memory, [collectionName, recordId, hashKey]);
    }
    async getMongo(collectionName, recordId, hashKey) {
        if (!this.mongoEnabled) return undefined;
        this.mongo.collection(collectionName).find({
            id: recordId
        }).toArray((err, docs) => {
            assert.equal(null, err);
            const value = docs[0] !== undefined ? docs[0][hashKey] : undefined;
            done(value);
        });
    }


////////////////////////////////////////////////////////////////////////////////
    async has(collectionName, recordId, hashKey) {
        assert.notEqual(undefined, collectionName);
        assert.notEqual(undefined, id);
        assert.notEqual(undefined, key);

        const hasMemDBPromise = this.hasMemDB(collectionm, recordId, hashKey);
        const hasRedisPromise = this.hasRedis(collectionm, recordId, hashKey);
        const hasLowDBPromise = this.hasLowDB(collectionm, recordId, hashKey);
        const hasMongoPromise = this.hasMongo(collectionm, recordId, hashKey);

        return await hasMemDBPromise === !undefined ? await hasMemDBPromise :
            await hasRedisPromise === !undefined ? await hasRedisPromise :
            await hasLowDBPromise === !undefined ? await hasLowDBPromise :
            await hasMongoPromise === !undefined ? await hasMongoPromise : false;
    }
    async hasMemDB(collectionName, recordId, hashKey) {
        if (!this.memDBEnabled) return undefined;
        return _.has(this.memory, [collectionName, recordId, hashKey]);
    }
    async hasRedis(collectionName, recordId, hashKey) {
        if (!this.redisEnabled) return undefined;
        // TODO:
        // [] Research redis "has"-like method support
        // [] Use redis has
        const value = await this.getRedis(collectionName, recordId, hashKey);
        return value !== null;
    }
    async hasLowDB(collectionName, recordId, hashKey) {
        if (!this.lowDBEnabled) return undefined;
        return this.lowDB.has(this.memory, [collectionName, recordId, hashKey]);
    }
    async hasMongo(collectionName, recordId, hashKey) {
        if (!this.mongoEnabled) return undefined;
        // TODO:
        // [] Research mongo "has"-like method support
        // [] Use mongo has
        const value = await this.getMongo(collectionName, recordId, hashKey);
        return value !== null;
    }


////////////////////////////////////////////////////////////////////////////////
    async set(collectionName, recordId, hashKey, value) {
        assert.notEqual(undefined, collectionName);
        assert.notEqual(undefined, id);
        assert.notEqual(undefined, key);

        const setMemDBPromise = this.setMemDB(collectionm, recordId, hashKey);
        const setRedisPromise = this.setRedis(collectionm, recordId, hashKey);
        const setLowDBPromise = this.setLowDB(collectionm, recordId, hashKey);
        const setMongoPromise = this.setMongo(collectionm, recordId, hashKey);

        await setMemDBPromise;
        await setRedisPromise;
        await setLowDBPromise;
        await setMongoPromise;
    }
    async setMemDB(collectionName, recordId, hashKey, value) {
        if (!this.memDBEnabled) return undefined;
        return _.set(this.memory, [collectionName, recordId, hashKey], value);
    }
    async setRedis(collectionName, recordId, hashKey, value) {
        if (!this.redisEnabled) return undefined;
        return await asyncly((done) => {
            this.redis.hset(`${collectionName}:${id}`, hashKey, value, (err, res) => {
                assert.equal(null, err);
                done();
            });
        });
    }
    async setLowDB(collectionName, recordId, hashKey, value) {
        if (!this.lowDBEnabled) return undefined;
        return this.lowDB.set(this.memory, [collectionName, recordId, hashKey], value);
    }
    async setMongo(collectionName, recordId, hashKey, value) {
        if (!this.mongoEnabled) return undefined;
        await asyncly(async (done) => {
            const patch = {};
            patch[hashKey] = value;
            this.mongo.updateOne({
                id: recordId
            }, {
                $set: patch
            }, (err, res) => {
                assert.equal(null, err);
                done();
            });
        });
    }
}

module.exports = exports = EZStore;