require("dotenv").config();
const { Client } = require("pg");
const client = require("./schema.i001");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("i001", () => {
  // Connect to PG
  const db = new Client({ connectionString });
  beforeAll(() => db.connect());
  afterAll(() => db.end());

  beforeEach(async () => {
    await client.reset(db);
    await client.create(db);
  });

  test("It should write into counters", async () => {
    await Promise.all([
      client.log(db, 'cnt1', 1),
      client.log(db, 'cnt1', 2),
      client.log(db, 'cnt2', 2),
      client.log(db, 'cnt1', -1),
      client.log(db, 'cnt2', 1),
    ])
    
    const r1 = await client.pack(db);
    expect(r1).toBe(5);

    expect(await client.getValue(db, 'cnt1')).toBe(2)
    expect(await client.getValue(db, 'cnt2')).toBe(3)
    
    // second round of increments

    await Promise.all([
      client.log(db, 'cnt1', 1),
      client.log(db, 'cnt2', -3),
    ])

    const r2 = await client.pack(db);
    expect(r2).toBe(2);

    expect(await client.getValue(db, 'cnt1')).toBe(3)
    expect(await client.getValue(db, 'cnt2')).toBe(0)
  });

  test('It should reset counters', async () => {
    await client.log(db, 'cnt1', 1);
    await client.log(db, 'cnt1', 2);
    await client.set(db, 'cnt2', 10);
    await client.log(db, 'cnt1', 3);
    await client.set(db, 'cnt1', 0);
    await client.log(db, 'cnt1', 1);
    await client.set(db, 'cnt1', 5);
    await client.set(db, 'cnt2', 6);

    const r1 = await client.pack(db);
    expect(r1).toBe(8);

    expect(await client.getValue(db, 'cnt1')).toBe(5)
    expect(await client.getValue(db, 'cnt2')).toBe(6)

    // add new logs with mixed reset and following increase

    await client.set(db, 'cnt1', 1);
    await client.log(db, 'cnt1', 1);
    await client.log(db, 'cnt2', -1);
    
    const r2 = await client.pack(db);
    expect(r2).toBe(3);

    expect(await client.getValue(db, 'cnt1')).toBe(2);
    expect(await client.getValue(db, 'cnt2')).toBe(5);
  });

  test('It should reset counters by pagination', async () => {
    await client.log(db, 'cnt1', 1);
    await client.log(db, 'cnt1', 2);
    await client.set(db, 'cnt2', 10);
    await client.log(db, 'cnt1', 3);
    await client.set(db, 'cnt1', 0);
    await client.log(db, 'cnt1', 1);
    await client.set(db, 'cnt1', 5);
    await client.set(db, 'cnt2', 6);
    await client.set(db, 'cnt1', 1);
    await client.log(db, 'cnt1', 1);
    await client.log(db, 'cnt2', -1);

    // The same result of the previous test should be achieved by
    // running an arbitrary page size
    const pageSize = 100;
    let results = await client.pack(db, pageSize);
    while (results > 0) {
      results = await client.pack(db, pageSize);
    }

    expect(await client.getValue(db, 'cnt1')).toBe(2);
    expect(await client.getValue(db, 'cnt2')).toBe(5);
  })
});
