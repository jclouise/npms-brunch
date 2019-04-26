const Worker = require('@scriptollc/amqp-worker')

const epoch = new Date('2019-04-20T08:21:00Z')

class NPMSBrunch extends Worker {
  constructor (queueName) {
    super()
    this.queue = queueName
  }

  async messageHandler (msg) {
    const message = JSON.parse(msg.content.toString())
    const msgDate = new Date(message.pushedAt)
    console.log('Handling message for', msgDate.toIsoString())
    if (msgDate < epoch) {
      this.channel.ack()
    } else {
      console.log('Hit epoch, bye')
      this.channel.nack()
    }
  }
}

async function brunch () {
  const queueName = 'npms'
  const worker = new NPMSBrunch(queueName)
  try {
    worker.listen()
  } catch (err) {
    console.log('ERR!', err)
  }
}

if (!module.parent) {
  brunch()
    .then(() => console.log('brunching'))
    .catch((err) => console.log('ERR!', err))
}
