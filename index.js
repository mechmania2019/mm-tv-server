const { promisify } = require("util");

const mongoose = require("mongoose");
const AWS = require("aws-sdk");
const { send, buffer } = require("micro");
const { Team, Match, Script } = require("mm-schemas")(mongoose);

const MIN_MATCH_LENGTH = 6;

mongoose.connect(process.env.MONGO_URL);
mongoose.Promise = global.Promise;

const s3 = new AWS.S3({
  params: { Bucket: "mechmania" }
});

const getObject = promisify(s3.getObject.bind(s3));

const randomItem = l => l[Math.floor(Math.random() * l.length)];

module.exports = async (req, res) => {
  console.log(`Getting all active teams`);
  const teams = await Team.find({})
    .populate("latestScript")
    .exec();
  let t1, t2, team1, team2, match;
  while (!match) {
    // Keep picking random matches till all conditions are met
    console.log(`Getting a random record from mongo`);
    team1 = randomItem(teams);
    team2 = randomItem(teams);
    if (team1 === team2 || !team1.latestScript || !team2.latestScript) {
      // Find another match
      continue;
    }
    // Set team names
    t1 = team1.name;
    t2 = team2.name;
    console.log(`Fetching matchup for ${t1} v ${t2}`);
    // Find the match record
    const gameRegex = `${team1.latestScript.key}:${team2.latestScript.key}|${
      team2.latestScript.key
    }:${team1.latestScript.key}`;
    match = await Match.findOne({ key: { $regex: gameRegex } })
      .where("length")
      .gt(MIN_MATCH_LENGTH)
      .exec();
  }
  console.log(`${match.key} - Found match`);
  console.log(`${t1} v ${t2} - Sending headers`);

  // Check whether our player1 id matches the one in the log
  const log_p1_key = match.key.slice("logs/".length).split(":")[0];
  if (log_p1_key === team1.latestScript.key) {
    res.setHeader("X-team-1", t1);
    res.setHeader("X-team-2", t2);
  } else {
    // Switch headers since our key is switched
    console.log("Teams were flipped because of key script locations");
    res.setHeader("X-team-1", t2);
    res.setHeader("X-team-2", t1);
  }

  console.log(`${t1} v ${t2} - Streaming match ${match.key} from s3`);
  try {
    return send(res, 200, s3.getObject({ Key: match.key }).createReadStream());
  } catch (e) {
    return send(res, 500, "Error when getting the logfile");
  }
};
