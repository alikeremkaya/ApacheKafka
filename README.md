# ApacheKafka

Bu depo, C# dilinde bir Kafka tüketici ve üretici uygulamasının implementasyonunu içermektedir. Proje yapısı şu şekildedir:

## Proje Yapısı

- **Kafka.Consumer**: Kafka tüketici implementasyonunu içerir.
  - **Events**
    - `MessageKey.cs`: Kafka mesajlarında kullanılan `MessageKey` kaydını tanımlar.
  - `CustomKeyDeSerializer.cs`: Kafka mesaj anahtarları için özel bir deserializer.
  - `CustomValueDeSerializer.cs`: Kafka mesaj değerleri için özel bir deserializer.
  - `Dockerfile`: Kafka tüketici uygulamasını derlemek ve çalıştırmak için Dockerfile.
  - `Properties/`: Proje özelliklerini içerir.
- **Kafka.Producer**: Kafka üretici implementasyonunu içerir.
  - **Events**: Üretici için olaylarla ilgili sınıfların yeri.
  - **DTOs**: Üretici için veri transfer nesnelerinin yeri.
- **Kafka.Consumer2**: Ek tüketici ile ilgili implementasyon için yer.

## Dosyalar

- **.dockerignore**: Docker derlemelerinde göz ardı edilecek dosya ve dizinleri belirtir.
- **.gitignore**: Git'te göz ardı edilecek dosya ve dizinleri belirtir.
- **.gitattributes**: Git için özellikleri tanımlar, örneğin satır sonu normalizasyonu gibi.

## Kurulum

### Gereksinimler

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- [Docker](https://www.docker.com/get-started)
- [Kafka](https://kafka.apache.org/quickstart)

### Derleme ve Çalıştırma

#### Docker Kullanarak

1. Kafka tüketici için Docker imajını oluşturun:

    ```sh
    docker build -t kafka-consumer -f Kafka.Consumer/Dockerfile .
    ```

2. Docker konteynerini çalıştırın:

    ```sh
    docker run --rm -it kafka-consumer
    ```

#### .NET CLI Kullanarak

1. Bağımlılıkları geri yükleyin:

    ```sh
    dotnet restore Kafka.Consumer/Kafka.Consumer.csproj
    ```

2. Projeyi derleyin:

    ```sh
    dotnet build Kafka.Consumer/Kafka.Consumer.csproj -c Release
    ```

3. Projeyi çalıştırın:

    ```sh
    dotnet run --project Kafka.Consumer/Kafka.Consumer.csproj
    ```

## Katkı Sağlama

Katkılar memnuniyetle karşılanır! Herhangi bir değişiklik için lütfen bir issue açın veya bir pull request gönderin.

## Lisans

Bu proje MIT Lisansı altında lisanslanmıştır.
