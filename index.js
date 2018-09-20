const { promisify } = require("util");

const mongoose = require("mongoose");
const AWS = require("aws-sdk");
const { send, buffer } = require("micro");
const { Match, Script } = require("mm-schemas")(mongoose);

const MIN_MATCH_LENGTH = 6;
const LAST_N_MATCHES_TO_PICK_FROM = 120;

mongoose.connect(process.env.MONGO_URL);
mongoose.Promise = global.Promise;

const s3 = new AWS.S3({
  params: { Bucket: "mechmania" }
});

const getObject = promisify(s3.getObject.bind(s3));

module.exports = async (req, res) => {
  // TODO: Prefer picking newer matches
  console.log(`Getting a random record from mongo`);
  const count = await Match.count()
    .where("length")
    .gt(MIN_MATCH_LENGTH)
    .limit(LAST_N_MATCHES_TO_PICK_FROM)
    .sort("-_id")
    .exec();
  console.log(`There are ${count} matches to pick from`);
  const random = Math.floor(Math.random() * count);
  console.log(`Grabbing record ${random}`);
  const match = await Match.findOne()
    .where("length")
    .gt(MIN_MATCH_LENGTH)
    .limit(LAST_N_MATCHES_TO_PICK_FROM)
    .sort("-_id")
    .skip(random)
    .exec();
  console.log(`${match.key} - Parsing match key`);
  const [s1, s2] = match.key.slice("logs/".length).split(":");
  console.log(`${s1} v ${s2} - Getting team names`);
  const [t1, t2] = await Promise.all(
    [s1, s2].map(async s => {
      const script = await Script.findOne({ key: s })
        .populate("owner")
        .exec();
      return script.owner.name;
    })
  );

  console.log(`${t1} v ${t2} - Sending headers`);
  res.setHeader("X-team-1", t1);
  res.setHeader("X-team-2", t2);

  console.log(`${t1} v ${t2} - Streaming match ${match.key} from s3`);
  try {
    return send(res, 200, s3.getObject({ Key: match.key }).createReadStream());
  } catch (e) {
    return send(res, 500, "Error when getting the logfile");
  }
};
