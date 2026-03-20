namespace DtPipe.Core.Options;

/// <summary>
/// Interface pour les ensembles d'options de provider qui nécessitent une requête SQL.
/// Permet au LinearPipelineService de définir la requête résolue sans passer par la réflexion.
/// </summary>
public interface IHasSqlQuery
{
    /// <summary>Définit la requête SQL résolue pour ce provider.</summary>
    void SetQuery(string query);
    
    /// <summary>Récupère la requête SQL actuelle.</summary>
    string? GetQuery();
}
