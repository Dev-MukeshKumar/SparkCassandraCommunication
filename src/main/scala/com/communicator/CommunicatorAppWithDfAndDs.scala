package com.communicator

import org.apache.log4j.Logger

object CommunicatorAppWithDfAndDs extends Serializable {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]):Unit = {
    logger.info("Communicator app started!")

    logger.info("Communicator app shutting down!")
  }
}
