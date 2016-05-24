package com.mnubo.flink.streaming

package object connectors {
  def using[T <: AutoCloseable, RES](resource: T)(action: T => RES) =
    try action(resource)
    finally resource.close()
}
