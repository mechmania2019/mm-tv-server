const { promisify } = require("util");

const mongoose = require("mongoose");
const AWS = require("aws-sdk");
const { send, buffer } = require("micro");
const { Match } = require("mm-schemas")(mongoose);

mongoose.connect(process.env.MONGO_URL);
mongoose.Promise = global.Promise;

const s3 = new AWS.S3({
  params: { Bucket: "mechmania" }
});

const getObject = promisify(s3.getObject.bind(s3));

module.exports = async (req, res) => {
  // TODO: Prefer picking newer matches
  console.log(`Getting a random record from mongo`);
  const count = await Match.count().exec();
  console.log(`There are ${count} matches to pick from`);
  const random = Math.floor(Math.random() * count);
  console.log(`Grabbing record ${random}`);
  const match = await Match.findOne()
    .skip(random)
    .exec();
  console.log(`${match.key} - Got a record`);

  console.log(`${match.key} - Downloading it from s3`);
  try {
    return send(res, 200, s3
      .getObject({ Key: match.key })
      .createReadStream());
  } catch (e) {
    return send(res, 500, "Error when getting the logfile")
  }
};
