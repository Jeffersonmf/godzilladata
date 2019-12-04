package exceptions

class LoadDataException(errorDetails: String) extends Exception {

  throw new Exception(String.format("Ocorreu um erro ao carregar os dados.: %s", errorDetails))
}
