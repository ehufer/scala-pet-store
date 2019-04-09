package io.github.pauljamescleary.petstore.infrastructure.grpc

import cats.MonadError
import cats.implicits._
import cats.effect.IO
import io.github.pauljamescleary.petstore.domain.PetAlreadyExistsError
import io.github.pauljamescleary.petstore.domain.pets.{Pet, PetService, PetStatus}
import io.github.pauljamescleary.petstore.proto
import io.github.pauljamescleary.petstore.proto.CreatePetRequest
import io.grpc
import io.grpc.StatusRuntimeException
import scalapb.GeneratedMessage

import scala.concurrent.Future

class PetServiceGrpcImpl(petService: PetService[IO]) extends proto.PetServiceGrpc.PetService {

  import ProtoTransformersSyntaxIO._
  override def createPet(request: proto.CreatePetRequest): Future[proto.Pet] = {
    val d = for {
      pet <- request.mapProtoTo[Pet]
      resp <- petService.create(pet).value
      protoPet <- resp.mapToProto[proto.Pet]
    } yield protoPet
    d.handleErrorWith(toGrpcStatus)
    d.unsafeToFuture
  }

  private def toGrpcStatus(error: Throwable) = error match {
    case grpcStatus: StatusRuntimeException => IO.raiseError(grpcStatus)
    case _ =>
      IO.raiseError(
        grpc.Status.INTERNAL
          .withDescription("Internal error creating a pet")
          .asRuntimeException())
  }

  override def updatePet(request: proto.UpdatePetRequest): Future[proto.Pet] = ???

  override def getPet(request: proto.GetPetRequest): Future[proto.Pet] = ???

  override def listPets(request: proto.ListPetsRequest): Future[proto.ListPetsResponse] = ???

  override def listPetsByStatus(
      request: proto.ListPetsByStatusRequest): Future[proto.ListPetsResponse] = ???

  override def listPetsByTag(request: proto.ListPetsByTagRequest): Future[proto.ListPetsResponse] =
    ???
}

trait FromProto[F[_], A <: GeneratedMessage, B] {
  def map(value: A)(implicit M: MonadError[F, Throwable]): F[B]
}

trait ToProto[F[_], A, B <: GeneratedMessage] {
  def map(value: A)(implicit M: MonadError[F, Throwable]): F[B]
}

object ProtoTransformersSyntaxIO extends ProtoTransformersSyntax[IO]

trait ProtoTransformersSyntax[F[_]] {

  implicit class FromProtoOps[A <: GeneratedMessage](value: A) {
    def mapProtoTo[B](implicit transformer: FromProto[F, A, B], M: MonadError[F, Throwable]): F[B] =
      transformer.map(value)
  }

  implicit class ToProtoOps[A](value: A) {
    def mapToProto[B <: GeneratedMessage](
        implicit transformer: ToProto[F, A, B],
        M: MonadError[F, Throwable]): F[B] =
      transformer.map(value)
  }

  implicit def CreateRequest_To_Pet: FromProto[F, CreatePetRequest, Pet] =
    new FromProto[F, proto.CreatePetRequest, Pet] {
      override def map(request: CreatePetRequest)(implicit M: MonadError[F, Throwable]): F[Pet] =
        request.pet match {
          case Some(protoPet) => ProtoPetToPet(protoPet).pure[F]
          case None =>
            grpc.Status.INVALID_ARGUMENT
              .withDescription("Invalid create pet request: No Pet")
              .asRuntimeException()
              .raiseError[F, Pet]
        }
    }

  private def ProtoPetToPet(protoPet: proto.Pet): Pet =
    Pet(
      name = protoPet.name,
      category = protoPet.category,
      bio = protoPet.bio,
      status = protoPet.petStatus match {
        case proto.Pet.Status.Adopted => PetStatus.Adopted
        case proto.Pet.Status.Available => PetStatus.Available
        case proto.Pet.Status.Pending => PetStatus.Pending
        case proto.Pet.Status.Unrecognized(_) =>
          PetStatus.Pending //TODO:Investigate how to ger rid of Unrecognized
      },
      tags = protoPet.tags.toSet,
      photoUrls = protoPet.photoUrls.toSet,
      id = protoPet.id.some
    )

  implicit def CreatePetResponse_To_ProtoPet
    : ToProto[F, Either[PetAlreadyExistsError, Pet], proto.Pet] =
    new ToProto[F, Either[PetAlreadyExistsError, Pet], proto.Pet] {
      override def map(response: Either[PetAlreadyExistsError, Pet])(
          implicit M: MonadError[F, Throwable]): F[proto.Pet] = response match {
        case Right(pet) => PetToProtoPet(pet).pure[F]
        case Left(PetAlreadyExistsError(_)) =>
          grpc.Status.ALREADY_EXISTS
            .withDescription(s"Pet already exists.")
            .asRuntimeException()
            .raiseError[F, proto.Pet]

      }
    }

  private def PetToProtoPet(pet: Pet): proto.Pet =
    proto.Pet(
      name = pet.name,
      category = pet.category,
      bio = pet.bio,
      petStatus = pet.status match {
        case PetStatus.Available => proto.Pet.Status.Available
        case PetStatus.Adopted => proto.Pet.Status.Adopted
        case PetStatus.Pending => proto.Pet.Status.Pending
      },
      tags = pet.tags.toSeq,
      photoUrls = pet.photoUrls.toSeq,
      id = pet.id.getOrElse(1) //TODO: Add wrapper
    )
}
