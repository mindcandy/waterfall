package com.mindcandy.waterfall.service

import argonaut.Argonaut._
import argonaut._
import spray.http.{ ContentTypeRange, ContentTypes, HttpCharsets, HttpEntity, MediaTypes }
import spray.httpx.marshalling.{ BasicMarshallers, Marshaller }
import spray.httpx.unmarshalling.{ Deserialized, MalformedContent, SimpleUnmarshaller, Unmarshaller }

trait ArgonautMarshallers extends BasicMarshallers {
  implicit val utf8StringMarshaller: Marshaller[String] =
    stringMarshaller(ContentTypes.`application/json`)

  implicit val utf8StringUnmarshaller: Unmarshaller[String] = new Unmarshaller[String] {
    def apply(entity: HttpEntity) = Right(entity.asString(defaultCharset = HttpCharsets.`UTF-8`))
  }

  implicit val argonautJsonMarshaller: Marshaller[Json] =
    Marshaller.delegate[Json, String](ContentTypes.`application/json`)(_.nospaces)

  implicit def argonautTMarshaller[T](implicit ev: EncodeJson[T]): Marshaller[T] =
    Marshaller.delegate[T, Json](ContentTypes.`application/json`)(param ⇒ {
      ev.encode(param)
    })

  implicit def argonautListTMarshaller[T](implicit ev: EncodeJson[List[T]]): Marshaller[List[T]] =
    Marshaller.delegate[List[T], Json](ContentTypes.`application/json`)(param ⇒
      ev.encode(param)
    )

  implicit val argonautJsonUnmarshaller: Unmarshaller[Json] =
    delegate[String, Json](MediaTypes.`application/json`)(string ⇒
      JsonParser.parse(string).toEither.left.map(error ⇒ MalformedContent(error))
    )

  implicit def argonautTUnmarshaller[T](implicit ev: DecodeJson[T]): Unmarshaller[T] =
    delegate[String, T](MediaTypes.`application/json`)(string ⇒
      string.decodeEither[T].toEither.left.map(error ⇒ MalformedContent(error))
    )

  implicit def argonautListTUnmarshaller[T](implicit ev: DecodeJson[List[T]]): Unmarshaller[List[T]] =
    delegate[String, List[T]](MediaTypes.`application/json`)(string ⇒
      string.decodeEither[List[T]].toEither.left.map(error ⇒ MalformedContent(error))
    )

  // Unmarshaller.delegate is used as a kind of map operation; argonaut can return an Either. We need a delegate method that works
  // as a flatMap and let the provided A ⇒ Deserialized[B] function to deal with any possible error, including exceptions.
  //
  private def delegate[A, B](unmarshalFrom: ContentTypeRange*)(f: A ⇒ Deserialized[B])(implicit ma: Unmarshaller[A]): Unmarshaller[B] =
    new SimpleUnmarshaller[B] {
      val canUnmarshalFrom = unmarshalFrom
      def unmarshal(entity: HttpEntity) = ma(entity).right.flatMap(a ⇒ f(a))
    }
}