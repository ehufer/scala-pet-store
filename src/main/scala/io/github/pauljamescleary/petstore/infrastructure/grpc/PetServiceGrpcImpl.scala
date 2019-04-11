package io.github.pauljamescleary.petstore.infrastructure.grpc

import cats.MonadError
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO
import io.github.pauljamescleary.petstore.domain.{PetAlreadyExistsError, PetNotFoundError}
import io.github.pauljamescleary.petstore.domain.pets.{Pet, PetService, PetStatus}
import io.github.pauljamescleary.petstore.proto
import io.grpc
import io.grpc.StatusRuntimeException
import scalapb.GeneratedMessage

import scala.concurrent.Future

class PetServiceGrpcImpl(petService: PetService[IO]) extends proto.PetServiceGrpc.PetService {

  import ProtoTransformersSyntaxIO._
  override def createPet(request: proto.CreatePetRequest): Future[proto.Pet] = {
    val resp = for {
      //_ <- breaker.verifyIsClosed//IO[Unit]
      pet <- request.mapProtoTo[Pet]
      result <- petService.create(pet).value
      protoResp <- result.mapToProto[proto.Pet]
    } yield protoResp
    toFuture(resp)
  }

  override def updatePet(request: proto.UpdatePetRequest): Future[proto.Pet] = {
    val resp = for {
      pet <- request.mapProtoTo[Pet]
      result <- petService.update(pet).value
      protoResp <- result.mapToProto[proto.Pet]
    } yield protoResp
    toFuture(resp)
  }

  override def getPet(request: proto.GetPetRequest): Future[proto.Pet] = {
    val resp = for {
      id <- request.mapProtoTo[Long]
      result <- petService.get(id).value
      protoResp <- result.mapToProto[proto.Pet]
    } yield protoResp
    toFuture(resp)
  }

  override def listPets(request: proto.ListPetsRequest): Future[proto.ListPetsResponse] = {
    val resp = for {
      pageSizeOffset <- request.mapProtoTo[(Int, Int)]
      //because withFilter is  unsafe. so with IO you can't (pageSize, offset) <- request.mapProtoTo[(Int, Int)]
      (pageSize, offset) = pageSizeOffset
      result <- petService.list(pageSize, offset)
      protoResp <- result.mapToProto[proto.ListPetsResponse]
    } yield protoResp
    toFuture(resp)
  }

  override def listPetsByStatus(request: proto.ListPetsByStatusRequest): Future[proto.ListPetsResponse] = {
    val resp = for {
      statuses <- request.mapProtoTo[NonEmptyList[PetStatus]]
      result <- petService.findByStatus(statuses)
      protoResp <- result.mapToProto[proto.ListPetsResponse]
    } yield protoResp
    toFuture(resp)
  }

  override def listPetsByTag(request: proto.ListPetsByTagRequest): Future[proto.ListPetsResponse] ={
    val resp = for {
      tags <- request.mapProtoTo[NonEmptyList[String]]
      result <- petService.findByTag(tags)
      protoResp <- result.mapToProto[proto.ListPetsResponse]
    } yield protoResp
    toFuture(resp)
  }

  private def toFuture[A](value: IO[A]): Future[A] = {
    value.handleErrorWith(toGrpcStatus)
    value.unsafeToFuture
  }

  private def toGrpcStatus(error: Throwable): IO[Nothing] = error match {
    case grpcStatus: StatusRuntimeException => IO.raiseError(grpcStatus)
    case _ =>
      IO.raiseError(
        grpc.Status.INTERNAL
          .withDescription("Internal error creating a pet")
          .asRuntimeException())
  }
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

  implicit def CreateRequestAsPet: FromProto[F, proto.CreatePetRequest, Pet] =
    new FromProto[F, proto.CreatePetRequest, Pet] {
      override def map(request: proto.CreatePetRequest)(
          implicit M: MonadError[F, Throwable]): F[Pet] =
        ProtoPetAsPet(request.pet)

    }

  implicit def UpdateRequestAsPet: FromProto[F, proto.UpdatePetRequest, Pet] =
    new FromProto[F, proto.UpdatePetRequest, Pet] {
      override def map(request: proto.UpdatePetRequest)(
          implicit M: MonadError[F, Throwable]): F[Pet] =
        ProtoPetAsPet(request.pet)
    }

  implicit def GetPetRequestAsLong: FromProto[F, proto.GetPetRequest, Long] =
    new FromProto[F, proto.GetPetRequest, Long] {
      override def map(request: proto.GetPetRequest)(
          implicit M: MonadError[F, Throwable]): F[Long] =
        request.id.pure[F]
    }

  implicit def ListPetsRequestAsPageSizeOffset: FromProto[F, proto.ListPetsRequest, (Int, Int)] =
    new FromProto[F, proto.ListPetsRequest, (Int, Int)] {
      override def map(request: proto.ListPetsRequest)(
          implicit M: MonadError[F, Throwable]): F[(Int, Int)] =
        (request.pageSize, request.offset).pure[F]
    }

  implicit def ListPetsByStatusRequestAsListStatuses
    : FromProto[F, proto.ListPetsByStatusRequest, NonEmptyList[PetStatus]] =
    new FromProto[F, proto.ListPetsByStatusRequest, NonEmptyList[PetStatus]] {
      override def map(request: proto.ListPetsByStatusRequest)(
          implicit M: MonadError[F, Throwable]): F[NonEmptyList[PetStatus]] =
        NonEmptyList.fromListUnsafe(request.statuses.map(ProtoStatusToPetStatus).toList).pure[F]
    }

  implicit def ListPetsByTagsRequestAsListStatuses
  : FromProto[F, proto.ListPetsByTagRequest, NonEmptyList[String]] =
    new FromProto[F, proto.ListPetsByTagRequest, NonEmptyList[String]] {
      override def map(request: proto.ListPetsByTagRequest)(
        implicit M: MonadError[F, Throwable]): F[NonEmptyList[String]] =
        NonEmptyList.fromListUnsafe(request.tags.toList).pure[F]
    }

  private def ProtoPetAsPet(maybeProtoPet: Option[proto.Pet])(
      implicit M: MonadError[F, Throwable]): F[Pet] =
    maybeProtoPet match {
      case Some(protoPet) => ProtoPetToPet(protoPet).pure[F]
      case None =>
        grpc.Status.INVALID_ARGUMENT
          .withDescription("Invalid create pet request: No Pet")
          .asRuntimeException()
          .raiseError[F, Pet]
    }

  private def ProtoPetToPet(protoPet: proto.Pet): Pet =
    Pet(
      name = protoPet.name,
      category = protoPet.category,
      bio = protoPet.bio,
      status = ProtoStatusToPetStatus(protoPet.petStatus),
      tags = protoPet.tags.toSet,
      photoUrls = protoPet.photoUrls.toSet,
      id = protoPet.id.some
    )

  private def ProtoStatusToPetStatus(protoStatus: proto.Pet.Status): PetStatus = protoStatus match {
    case proto.Pet.Status.Adopted => PetStatus.Adopted
    case proto.Pet.Status.Available => PetStatus.Available
    case proto.Pet.Status.Pending => PetStatus.Pending
    case proto.Pet.Status.Unrecognized(_) =>
      PetStatus.Pending //TODO:Investigate how to ger rid of Unrecognized
  }

  implicit def CreatePetResponseAsProtoPet
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

  implicit def UpdatePetResponseAsProtoPet
    : ToProto[F, Either[PetNotFoundError.type, Pet], proto.Pet] =
    new ToProto[F, Either[PetNotFoundError.type, Pet], proto.Pet] {
      override def map(response: Either[PetNotFoundError.type, Pet])(
          implicit M: MonadError[F, Throwable]): F[proto.Pet] = response match {
        case Right(pet) => PetToProtoPet(pet).pure[F]
        case Left(PetNotFoundError) =>
          grpc.Status.NOT_FOUND
            .withDescription(s"Pet doesn't exist.")
            .asRuntimeException()
            .raiseError[F, proto.Pet]

      }
    }

  implicit def ListPetsResponseAsListPetsResponseProto
    : ToProto[F, List[Pet], proto.ListPetsResponse] =
    new ToProto[F, List[Pet], proto.ListPetsResponse] {
      override def map(response: List[Pet])(
          implicit M: MonadError[F, Throwable]): F[proto.ListPetsResponse] =
        proto.ListPetsResponse(pets = response.map(PetToProtoPet)).pure[F]

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
